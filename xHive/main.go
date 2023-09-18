package main

import (
	"context"
	"fmt"
	"github.com/beltran/gohive"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"
)

func printBanner() {
	fmt.Println("\n       _    _ _______      ________ \n      | |  | |_   _\\ \\    / /  ____|\n __  _| |__| | | |  \\ \\  / /| |__   \n \\ \\/ /  __  | | |   \\ \\/ / |  __|  \n  >  <| |  | |_| |_   \\  /  | |____ \n /_/\\_\\_|  |_|_____|   \\/   |______|\n")
}

// 配置文件的结构体
type Config struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Auth     string `yaml:"auth"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

// 配置文件加载函数
func loadConfig(filePath string) (*Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开配置文件失败，错误是: %w", err)
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败，错误是: %w", err)
	}

	config := &Config{}
	err = yaml.Unmarshal(data, config)
	if err != nil {
		return nil, fmt.Errorf("解析配置文件失败，错误是: %w", err)
	}

	return config, nil
}

// 创建存储表结构的SQL文件
func createFile(saveName string) (*os.File, error) {
	file, err := os.Create(saveName)
	if err != nil {
		return nil, fmt.Errorf("创建文件失败，错误为: %w", err)
	}
	return file, nil
}

// 将表结构写入文件
func writeToFile(file *os.File, tableName string, tableStructure string) error {
	_, err := file.WriteString("\n-- " + tableName + "\n" + tableStructure + ";\n\n")
	if err != nil {
		return fmt.Errorf("写入文件失败，错误为: %w", err)
	}
	return nil
}

// 连接数据库并循环存入所需表结构
func startWork() {
	// 打开配置文件
	filePath, err := filepath.Abs("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// 加载配置文件
	config, err := loadConfig(filePath)
	if err != nil {
		log.Fatal(err)
	}

	// 初始化存储的SQL文件
	saveName := config.Database + "_" + time.Now().Format("20060102") + ".sql"

	log.Printf("开始连接数据库...")
	log.Printf("数据库地址：%s:%d", config.Host, config.Port)
	log.Printf("数据库实例：%v", config.Database)
	switch config.Auth {
	case "kerberos":
		log.Printf("认证方式为：Sasl kerberos")
	case "NONE":
		log.Printf("认证方式为：Plain Sasl")
	case "NOSASL":
		log.Printf("认证方式为：No Sasl")
	default:
		log.Printf("大哥别看了，你看看你认证方式写没写！！！")
	}

	// 数据库连接信息
	configuration := gohive.NewConnectConfiguration()
	configuration.Username = config.Username
	configuration.Password = config.Password
	configuration.Database = config.Database

	log.Println("正在连接数据库...")
	startTime := time.Now()
	connection, err := gohive.Connect(config.Host, config.Port, config.Auth, configuration)
	if err != nil {
		log.Fatalf("连接数据库失败，错误为: %v", err)

	}
	defer connection.Close()
	log.Println("成功连接到数据库！耗时:", time.Since(startTime))

	//--------------------以下开始正式的工作----------------------------------
	// 连接完成，开始数据库语句的使用

	// 创建保存结构体的SQL的文件
	file, err := createFile(saveName)
	if err != nil {
		log.Fatalf("创建文件失败，错误为: %v", err)
	}
	defer file.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	cursor := connection.Cursor()
	defer cursor.Close()

	// 执行 SHOW TABLES 查询
	cursor.Exec(ctx, "SHOW TABLES")

	// 循环打印
	var count int
	for cursor.HasMore(ctx) {
		var tableName string
		count++
		cursor.FetchOne(ctx, &tableName)
		if cursor.Err != nil {
			log.Fatalf("获取表名失败，错误为: %v", cursor.Err)
		}
		// 重置表结构字符串
		tableStructure := ""
		// 循环打印出表结构
		stmt := fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)

		// 创建新的游标用于获取表结构
		structureCursor := connection.Cursor()
		defer structureCursor.Close()

		structureCursor.Exec(ctx, stmt)
		if err != nil {
			log.Fatalf("获取表结构失败，错误为: %v", err)
		}

		for structureCursor.HasMore(ctx) {
			var tableStructureLine string
			structureCursor.FetchOne(ctx, &tableStructureLine)
			if structureCursor.Err != nil {
				log.Fatalf("获取表结构失败，错误为: %v", structureCursor.Err)
			}
			tableStructure += tableStructureLine + "\n"
		}

		// 写入到文件的函数
		log.Printf("正在导出第%v个的表，表名为：%s", count, config.Database+"."+tableName)
		err = writeToFile(file, config.Database+"."+tableName, tableStructure)
		if err != nil {
			log.Fatalf("写入文件失败，错误为：%v", err)

		}
	}
	log.Printf("\n本次共导出表结构%v个\n", count)
	log.Printf("\n%v完成写入SQL文件!", config.Database)
	log.Println("表结构保存在SQL文件：", saveName)

}

func main() {
	printBanner()
	time.Sleep(1 * time.Second)
	log.Println("xHIVE--开始工作...")
	//开始干活！！！
	startWork()
}

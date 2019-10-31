package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mgutz/str"
	"github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)
// 先进先出

const HANDLE_DIG = "/dig"

const HANDLE_MOVIE = "/movie/"
const HANDLE_LIST = "/list/"
const HANDLE_HTML = ".html"

type cmdParams struct {
	logFilePath string
	routineNum int

}

type digData struct {
	time string
	url string
	refer string
	ua string
}

type urlData struct {
	data digData
	uid string
	uNode urlNode
}
// 用来做存储
type urlNode struct {
	unType  string
	unRid  	int
	unUrl 	string
	unTime 	string
}

type storageBlock struct {
	counterType  string
	storageModel string
	unode       urlNode
}
var log = logrus.New()
func init(){
	//  日志的标准输出
	log.Out = os.Stdout
	// 日志等级  info  waring
	log.SetLevel(logrus.DebugLevel)
}

func main(){
	// 获取参数

	logFilePath := flag.String("logFilePath","C:/phpstudy_pro/WWW/gotest/src/dailybtc/dig.log","log file path")
	routineNum := flag.Int("routineNum",5,"runtine num value is five")

	l := flag.String("l","C:/phpstudy_pro/WWW/gotest/src/analytics/dig.log","this is path log ")
	flag.Parse()

	params := cmdParams{*logFilePath,*routineNum}
	// 指定打日志文件
	logFd,err := os.OpenFile(*l,os.O_CREATE|os.O_WRONLY,0644)
	if err ==nil{
		log.Out = logFd
		defer logFd.Close()
	}

	log.Infof("exec start")
	fmt.Println(*routineNum)
	log.Infof("params:logFilePath=%s,routineNum=%d",*logFilePath,*routineNum)

	// 打印日志

	// 初始化一些channel  用于数据传递

	var logChannel = make(chan string,5*params.routineNum)
	var uvChannel = make(chan urlData,params.routineNum)
	var pvChannel = make(chan urlData,params.routineNum)
	var storageChannel = make(chan storageBlock,params.routineNum)

	// 创建redis连接池

	redisPool ,err := pool.New("tcp","127.0.0.1:6379",10)

	if err != nil {
		log.Fatalln("redis piool created failed")
		panic(err)
	}else{
		// 持续ping   保持连接池
		go func(){
			for{
				redisPool.Cmd("PING")
				time.Sleep(3*time.Second)
			}
		}()
	}

	//  日志消费者

	go readFileLineByLine(params,logChannel)

	// 创建以组日志处理
	for i:=0; i<params.routineNum;i++{
		go logConsumer(logChannel,pvChannel,uvChannel)
	}

	// 创建pv uv  统计器

	go pvCounter(pvChannel,storageChannel)
	go uvCounter(uvChannel,storageChannel,redisPool)

	// 创建存储器
	go dataStorage(storageChannel,redisPool)
	// 协程结束后会退出程序  time用来调试
	time.Sleep(1000*time.Second)

}
func pvCounter(pvChannel chan urlData,storageChannel chan storageBlock){
	for data := range pvChannel{
		sItem := storageBlock{"pv","ZINCRBY",data.uNode}
		storageChannel <- sItem
	}
}

func uvCounter(uvChannel chan urlData,storageChannel chan storageBlock,redisPool *pool.Pool){
	for data := range uvChannel{
		hyperLogLogKey := "uv_hpll_"+getTime(data.data.time,"day")
		ret, err := redisPool.Cmd("PFADD",hyperLogLogKey,data.uid,"EX",86400).Int()
		if err != nil {
			log.Warningln("uv check hyperLoglog redis is value",err)
		}
		if ret!=1 {
			continue
		}
		sItem := storageBlock{"uv","ZINCRBY",data.uNode}
		storageChannel <- sItem
	}
}

func dataStorage(storageChannel chan storageBlock,redisPool *pool.Pool){
	for block := range storageChannel{
		prefix := block.counterType + "_"
		setKeys := []string{
			prefix + "day_"+getTime(block.unode.unTime,"day"),
			prefix + "hour_"+getTime(block.unode.unTime,"hour"),
			prefix + "min_"+getTime(block.unode.unTime,"min"),
			prefix + block.unode.unType+ "day_"+ getTime(block.unode.unTime,"day"),
			prefix + block.unode.unType+ "hour_"+ getTime(block.unode.unTime,"hour"),
			prefix + block.unode.unType+ "min_"+ getTime(block.unode.unTime,"min"),
		}

		rowId :=  block.unode.unRid
		for _,key :=  range setKeys{
			ret,err := redisPool.Cmd(block.storageModel,key,1,rowId).Int()
			if ret <= 0 || err != nil {
				log.Errorf("datastorge redis storage err .",block.storageModel,key,rowId)
			}
		}

	}
}

func readFileLineByLine(params cmdParams,logChannel chan string) error{
	fd,err := os.Open(params.logFilePath)
	if err != nil{
		log.Warningf("readfile is fail:%s",params.logFilePath)
		return err
	}
	defer fd.Close()
	count := 0
	buffeRead := bufio.NewReader(fd)
	for{
		lines ,err := buffeRead.ReadString('\n')
		// 取出数据放到通道 然后消费当前的日志
		logChannel <- lines
		//log.Infof("line:",lines)
		count++
		if count%(1000*params.routineNum) == 0{
			log.Infof("readfileline:%d",count)
		}
		if err != nil {
			if err == io.EOF{
				time.Sleep(3*time.Second)
				log.Infof("readfileLine wait ,readline",count)
			}else{
				log.Warning("ReadFileLine read waring",count)
			}
		}
	}
	return nil

}

func logConsumer(logChannel chan string, pvChannel ,  uvChannel chan urlData)error{
	for logStr := range logChannel{
		// 处理字符串   上报的数据格式
		data := dealLogData(logStr)
		// 模拟uid
		hasher := md5.New()
		hasher.Write([]byte(data.refer+data.ua))
		uid := hex.EncodeToString(hasher.Sum(nil))
		// 解析处理后的数据
		uData := urlData{data,uid,formatUrl(data.url,data.time)}

		//log.Infoln(uData)

		pvChannel <- uData
		uvChannel <- uData
	}
	return  nil
}

func formatUrl(url,t string) urlNode{
	pos1 := str.IndexOf(url,HANDLE_MOVIE,0)
	if pos1!= -1{
		pos1 += len(HANDLE_MOVIE)
		pos2 := str.IndexOf(url,HANDLE_HTML,0)
		idStr := str.Substr(url,pos1,pos2-pos1)
		id,_ := strconv.Atoi(idStr)
		return  urlNode{"movie",id,url,t}
	}else{
		pos1 = str.IndexOf(url,HANDLE_LIST,0)
		if pos1 != -1 {
			pos1 += len(HANDLE_LIST)
			pos2 := str.IndexOf(url,HANDLE_HTML,0)
			idStr := str.Substr(url,pos1,pos2-pos1)
			id,_ := strconv.Atoi(idStr)
			return  urlNode{"movie",id,url,t}
		}else{
			return urlNode{"home",1,url,t}
		}
	}
}

func dealLogData(logStr string) digData {
	logStr  = strings.TrimSpace(logStr)
	pos1 := str.IndexOf(logStr,HANDLE_DIG,0)
	if pos1 == -1{
		return digData{}
	}
	pos1 += len(HANDLE_DIG)
	pos2 := str.IndexOf(logStr,"HTTP/",pos1)
	d := str.Substr(logStr,pos1,pos2-pos1)
	urlInfo ,err := url.Parse("http://locahost/?"+d)
	if err != nil {
		return  digData{}
	}
	data := urlInfo.Query()
	return digData{
		time:  data.Get("time"),
		url:   data.Get("url"),
		refer: data.Get("refer"),
		ua:    data.Get("ua"),
	}

}

func getTime(logTime,timeType string) string{
	var item string
	switch timeType {
		case  "day":
			item = "2006-01-02"
			break
		case  "hour":
			item = "2006-01-02 15"
			break
		case  "min":
			item = "2006-01-02 15:03"
			break
	}
	t,_ := time.Parse(item,time.Now().Format(item))
	return  strconv.FormatInt(t.Unix(),10)
}

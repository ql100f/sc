package channel

import (
	"log"
	"sync"
	"time"
)

func Run_b() {
	wg := &sync.WaitGroup{}
	ch := make(chan int)
	stop := make(chan struct{})
	go receive_b(ch, stop)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go send_b(wg, ch, i)
	}
	wg.Wait()
	close(ch) //发送完了，关闭通道
	<-stop    // 不管send需要多久，都会一直等到处理完，关闭stop通道，这行才会执行下去
	log.Println("good all data handle over")
}

func receive_b(ch chan int, stop chan struct{}) {
	for v := range ch {
		log.Println("receive:", v)
	}
	time.Sleep(1 * time.Second)
	log.Println("接收完毕")
	close(stop)
}

func send_b(wg *sync.WaitGroup, ch chan int, index int) {
	defer wg.Done()
	for i := 0; i < 5; i++ {
		ch <- index*10 + i
	}
}

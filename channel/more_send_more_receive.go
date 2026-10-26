package channel

import (
	"log"
	"sync"
	"time"
)

func Run_c() {
	wg := &sync.WaitGroup{}
	wg2 := &sync.WaitGroup{}
	ch := make(chan int)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go send_c(wg, ch, i)
	}

	for i := 0; i < 5; i++ {
		wg2.Add(1)
		go receive_c(wg2, ch, i)
	}

	wg.Wait()
	close(ch)
	wg2.Wait()
	log.Println("ok")
}

func send_c(wg *sync.WaitGroup, ch chan int, index int) {
	defer wg.Done()
	for i := 0; i < 5; i++ {
		ch <- index*10 + i
		time.Sleep(100 * time.Millisecond)
	}
}

func receive_c(wg *sync.WaitGroup, ch chan int, index int) {
	defer wg.Done()
	for v := range ch {
		log.Println("receive:", v, " ", index)
	}
}

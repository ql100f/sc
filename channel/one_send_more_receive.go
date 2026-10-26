package channel

import (
	"fmt"
	"sync"
	"time"
)

func Run_a() {
	wg := &sync.WaitGroup{}
	ch := make(chan int)
	go send_a(ch)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go receive_a(wg, ch, i)
	}

	wg.Wait()

}

func send_a(ch chan int) {
	for i := 0; i < 10; i++ {
		ch <- i
		time.Sleep(200 * time.Millisecond)
	}
	close(ch)
}

func receive_a(wg *sync.WaitGroup, ch chan int, index int) {
	defer wg.Done()
	for v := range ch {
		fmt.Println("receive :", v)
	}
	fmt.Println("receive done :", index)
}

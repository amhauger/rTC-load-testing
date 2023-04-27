package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

// we're going to run a bunch of go routines
// 1. every x seconds we are going to queue a car to the rTC
// 2. every y seconds we are going to get the queue from the rTC
// 3. every z seconds we are going to swap a vehicle from place 1 to place rand int [2:len(queue)]
func main() {
	// flags
	queueCar := flag.Int("queue", 2, "number of seconds between car queueing")
	getQueue := flag.Int("get", 4, "number of seconds between calls to get queue")
	moveCar := flag.Int("move", 6, "number of seconds between calls to move lead car")
	rtcHost := flag.String("client", "192.168.1.80", "ip of rTC")
	rtcPort := flag.Int("port", 20250, "port for rTC")

	flag.Parse()

	// csv creation
	t := time.Now().String()
	date, err := time.Parse("DateOnly", t)
	if err != nil {
		log.Fatal().Err(err).Msg("error parsing current time to date only")
		panic(err)
	}
	time, err := time.Parse("TimeOnly", t)
	if err != nil {
		log.Fatal().Err(err).Msg("error parsing current time to time only")
		panic(err)
	}

	fileName := fmt.Sprintf("%s/%s/load-test.csv", date, time)
	_, err = os.Stat(fileName)
	var f *os.File
	if os.IsNotExist(err) {
		f, err = os.Create(fileName)
	} else {
		fileName = fmt.Sprintf("%s+1", fileName)
		f, err = os.Create(fileName)
	}

	if err != nil {
		log.Fatal().Err(err).Str("fileName", fileName).Msg("unable to create csv file")
		panic(err)
	}

	csvWriter := csv.NewWriter(f)
	err = csvWriter.Write([]string{"rTC Command", "Connected", "Command Initiated", "Command Retrieved", "Closed", "Error", "Error Message"})
	if err != nil {
		log.Fatal().Err(err).Str("fileName", fileName).Msg("error writing headers to csv file")
		panic(err)
	}

	// create and run routines
	routines := CreateRoutines(*queueCar, *getQueue, *moveCar)
	routines.RTC = CreateRTCClient(*rtcHost, *rtcPort)
	routines.Writer = csvWriter
	routines.RunAll()

	r := gin.New()
	r.GET("/stop", routines.StopAll)
	r.GET("/stop/queue-and-move", routines.StartQueueAndMove)
	r.GET("/start/queue-and-move", routines.StartQueueAndMove)
	r.GET("/delete", routines.DeleteQueuedCars)
	r.GET("/update/queue/:seconds", routines.UpdateQueueTime)
	r.GET("/update/move/:seconds", routines.UpdateMoveTime)
	r.GET("/update/get/:seconds", routines.UpdateGetTime)
	r.GET("/update/:queueTime/:moveTime/:getTime", routines.UpdateAllTimes)

	// start server
	log.Fatal().Err(r.Run(":3001"))
}

type Routines struct {
	*QueueRoutine
	*GetRoutine
	*MoveRoutine
	RTC    *RTCClient
	Writer *csv.Writer
}

func CreateRoutines(queueTime, getTime, moveTime int) *Routines {
	queueDone := make(chan bool)
	getDone := make(chan bool)
	moveDone := make(chan bool)

	q := CreateQueueRoutine(queueTime, queueDone)
	g := CreateGetRoutine(getTime, getDone)
	m := CreateMoveRoutine(moveTime, moveDone)

	return &Routines{
		QueueRoutine: q,
		GetRoutine:   g,
		MoveRoutine:  m,
	}
}

func (r *Routines) RunAll() {
	go r.QueueRoutine.Run(r.RTC, r.Writer)
	log.Info().Msg("queue routine started")

	go r.GetRoutine.Run(r.RTC, r.Writer)
	log.Info().Msg("get routine started")

	go r.MoveRoutine.Run(r.RTC, r.Writer)
	log.Info().Msg("move routine started")
}

func (r *Routines) StopAll(c *gin.Context) {
	r.QueueRoutine.Done <- true
	r.GetRoutine.Done <- true
	r.MoveRoutine.Done <- true

	c.Redirect(http.StatusOK, "/delete")
}

func (r *Routines) StopQueueAndMove(c *gin.Context) {
	r.QueueRoutine.Done <- true
	r.MoveRoutine.Done <- true

	c.Redirect(http.StatusOK, "/delete")
}

func (r *Routines) StartQueueAndMove(c *gin.Context) {
	go r.QueueRoutine.Run(r.RTC, r.Writer)
	log.Info().Msg("queue routine started")

	go r.MoveRoutine.Run(r.RTC, r.Writer)
	log.Info().Msg("move routine started")
}

func (r *Routines) DeleteQueuedCars(c *gin.Context) {
	queue, times, err := r.RTC.GetQueue()
	writeErr := r.Writer.Write(times)
	if writeErr != nil {
		log.Warn().Err(err).Strs("record", times).Msg("error writing get queue record to CSV")
	}

	if err != nil {
		log.Error().Err(err).Msg("error getting queue to delete all washes queued by routine")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch rtc queue"})
		return
	}

	for _, wash := range queue.Queue.QueueItems {
		if wash.WashPkgNum == 1 {
			times, err := r.RTC.DeleteQueuedCar(wash.WashID)
			writeErr := r.Writer.Write(times)
			if writeErr != nil {
				log.Warn().Err(err).Strs("record", times).Msg("error writing delete record to CSV")
			}

			if err != nil {
				log.Error().Err(err).Interface("wash", wash).Msg("error deleting wash from queue")
			}
		}
	}
}

func (r *Routines) UpdateQueueTime(c *gin.Context) {
	s := c.Param("seconds")
	if s == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no time span specified"})
		return
	}
	r.QueueRoutine.UpdateTime(s)
	r.QueueRoutine.Run(r.RTC, r.Writer)
	log.Info().Str("newTickerTime", s).Msg("successfully updated queue routine's ticker time")
}

func (r *Routines) UpdateMoveTime(c *gin.Context) {
	s := c.Param("seconds")
	if s == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no time span specified"})
		return
	}
	r.MoveRoutine.UpdateTime(s)
	go r.MoveRoutine.Run(r.RTC, r.Writer)
	log.Info().Str("newTickerTime", s).Msg("successfully updated move routine's ticker time")
}

func (r *Routines) UpdateGetTime(c *gin.Context) {
	s := c.Param("seconds")
	if s == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no time span specified"})
		return
	}
	r.GetRoutine.UpdateTime(s)
	go r.GetRoutine.Run(r.RTC, r.Writer)
	log.Info().Str("newTickerTime", s).Msg("successfully updated get routine's ticker time")
}

func (r *Routines) UpdateAllTimes(c *gin.Context) {
	q := c.Param("queueTime")
	if q == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no time span specified for queue timer"})
		return
	}

	m := c.Param("moveTime")
	if m == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no time span specified for move timer"})
		return
	}

	g := c.Param("getTime")
	if g == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no time span specified for get timer"})
		return
	}

	r.QueueRoutine.UpdateTime(q)
	r.MoveRoutine.UpdateTime(m)
	r.GetRoutine.UpdateTime(g)
	r.RunAll()
}

type QueueRoutine struct {
	Done   chan bool
	Ticker *time.Ticker
}

func CreateQueueRoutine(tickerTime int, doneChannel chan bool) *QueueRoutine {
	t := strconv.Itoa(tickerTime)

	d, err := time.ParseDuration(t)
	if err != nil {
		log.Error().Err(err).Int("tickerTime", tickerTime).Str("convertedTime", t).Msg("error converting queue car time string to time.duration; forcing ticker duration to be default")
		d = 2
	}
	return &QueueRoutine{
		Done:   doneChannel,
		Ticker: time.NewTicker(d * time.Second),
	}
}

func (q *QueueRoutine) Run(client *RTCClient, writer *csv.Writer) {
	for {
		select {
		case <-q.Done:
			log.Info().Msg("queue routine received done signal")
			return
		case <-q.Ticker.C:
			req := WashRequest{
				LaneID:      "4",
				OrderID:     "LOAD-TESTING",
				VehicleID:   "NO-VALID-ID",
				WashPackage: 1,
			}

			records, err := client.QueueWash(req)
			if err != nil {
				log.Warn().Err(err).Msg("unable to queue wash in queue routine")
			}
			writer.Write(records)
		}
	}
}

func (q *QueueRoutine) UpdateTime(tickerTime string) {
	q.Done <- true

	d, err := time.ParseDuration(tickerTime)
	if err != nil {
		log.Error().Err(err).Str("tickerTime", tickerTime).Msg("error converting queue car time string to time.duration; forcing ticker duration to be default")
		d = 2
	}
	q.Ticker = time.NewTicker(d * time.Second)
}

type GetRoutine struct {
	Done   chan bool
	Ticker *time.Ticker
}

func CreateGetRoutine(tickerTime int, doneChannel chan bool) *GetRoutine {
	t := strconv.Itoa(tickerTime)

	d, err := time.ParseDuration(t)
	if err != nil {
		log.Error().Err(err).Int("tickerTime", tickerTime).Str("convertedTime", t).Msg("error converting get queue time string to time.duration; forcing ticker duration to be default")
		d = 4
	}
	return &GetRoutine{
		Done:   doneChannel,
		Ticker: time.NewTicker(d * time.Second),
	}
}

func (g *GetRoutine) Run(client *RTCClient, writer *csv.Writer) {
	for {
		select {
		case <-g.Done:
			log.Info().Msg("get routine received done signal")
			return
		case <-g.Ticker.C:
			_, records, err := client.GetQueue()
			if err != nil {
				log.Warn().Err(err).Msg("unable to get rtc queue in get queue routine")
			}
			writer.Write(records)
		}
	}
}

func (g *GetRoutine) UpdateTime(tickerTime string) {
	g.Done <- true

	d, err := time.ParseDuration(tickerTime)
	if err != nil {
		log.Error().Err(err).Str("tickerTime", tickerTime).Msg("error converting get queue time string to time.duration; forcing ticker duration to be default")
		d = 4
	}
	g.Ticker = time.NewTicker(d * time.Second)
}

type MoveRoutine struct {
	Done   chan bool
	Ticker *time.Ticker
}

func CreateMoveRoutine(tickerTime int, doneChannel chan bool) *MoveRoutine {
	t := strconv.Itoa(tickerTime)

	d, err := time.ParseDuration(t)
	if err != nil {
		log.Error().Err(err).Int("tickerTime", tickerTime).Str("convertedTime", t).Msg("error converting move car time string to time.duration; forcing ticker duration to be default")
		d = 6
	}
	return &MoveRoutine{
		Done:   doneChannel,
		Ticker: time.NewTicker(d * time.Second),
	}
}

func (m *MoveRoutine) Run(client *RTCClient, writer *csv.Writer) {
	for {
		select {
		case <-m.Done:
			log.Info().Msg("move routine received done signal")
			return
		case <-m.Ticker.C:
		}
	}
}

func (m *MoveRoutine) UpdateTime(tickerTime string) {
	m.Done <- true

	d, err := time.ParseDuration(tickerTime)
	if err != nil {
		log.Error().Err(err).Str("tickerTime", tickerTime).Msg("error converting move car time string to time.duration; forcing ticker duration to be default")
		d = 6
	}
	m.Ticker = time.NewTicker(d * time.Second)
}

package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
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
	queueCar := flag.Float64("queue", 2, "number of seconds between car queueing")
	getQueue := flag.Float64("get", 4, "number of seconds between calls to get queue")
	moveCar := flag.Float64("move", 6, "number of seconds between calls to move lead car")
	rtcHost := flag.String("client", "192.168.1.80", "ip of rTC")
	rtcPort := flag.Int("port", 20250, "port for rTC")

	flag.Parse()

	// csv creation
	t := time.Now()
	date := t.Format(time.DateOnly)
	time := t.Format(time.TimeOnly)

	fileName := fmt.Sprintf("load-test-%s-%s.csv", date, time)
	f, err := os.Create(fileName)

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
	routines.Writer = &Writer{
		Writer:  csvWriter,
		Records: make(chan []string, 100),
		Done:    make(chan bool),
	}
	routines.RunAll()

	r := gin.New()
	r.GET("/stop", routines.StopAll)
	r.GET("/start", routines.StartAll)
	r.GET("/start/get", routines.StartGet)
	r.GET("/stop/queue-and-move", routines.StopQueueAndMove)
	r.GET("/start/queue-and-move", routines.StartQueueAndMove)
	r.GET("/delete", routines.DeleteQueuedCars)
	r.GET("/update/queue/:seconds", routines.UpdateQueueTime)
	r.GET("/update/move/:seconds", routines.UpdateMoveTime)
	r.GET("/update/get/:seconds", routines.UpdateGetTime)
	r.GET("/update/:queueTime/:moveTime/:getTime", routines.UpdateAllTimes)

	// start server
	log.Fatal().Err(r.Run(":3001"))
}

type Writer struct {
	Writer  *csv.Writer
	Records chan []string
	Done    chan bool
}

func (w *Writer) Write() {
	for {
		select {
		case <-w.Done:
			log.Info().Msg("write routine received done signal")
			return
		case <-w.Records:
			record := <-w.Records
			log.Info().Strs("record", record).Msg("writing record to csv")
			w.Writer.Write(record)
		}
	}
}

type Routines struct {
	*QueueRoutine
	*GetRoutine
	*MoveRoutine
	RTC    *RTCClient
	Writer *Writer
}

func CreateRoutines(queueTime, getTime, moveTime float64) *Routines {
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

func (r *Routines) StartAll(c *gin.Context) {
	r.RunAll()
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (r *Routines) RunAll() {
	go r.QueueRoutine.Run(r.RTC, r.Writer)
	log.Info().Msg("queue routine started")

	go r.GetRoutine.Run(r.RTC, r.Writer)
	log.Info().Msg("get routine started")

	go r.MoveRoutine.Run(r.RTC, r.Writer)
	log.Info().Msg("move routine started")

	go r.Writer.Write()
	log.Info().Msg("records writer routine started")
}

func (r *Routines) StopAll(c *gin.Context) {
	r.QueueRoutine.Done <- true
	r.GetRoutine.Done <- true
	r.MoveRoutine.Done <- true
	r.Writer.Done <- true

	c.Redirect(http.StatusFound, "/delete")
}

func (r *Routines) StartGet(c *gin.Context) {
	go r.GetRoutine.Run(r.RTC, r.Writer)

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (r *Routines) StopQueueAndMove(c *gin.Context) {
	r.QueueRoutine.Done <- true
	r.MoveRoutine.Done <- true

	c.Redirect(http.StatusFound, "/delete")
}

func (r *Routines) StartQueueAndMove(c *gin.Context) {
	go r.QueueRoutine.Run(r.RTC, r.Writer)
	log.Info().Msg("queue routine started")

	go r.MoveRoutine.Run(r.RTC, r.Writer)
	log.Info().Msg("move routine started")

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (r *Routines) DeleteQueuedCars(c *gin.Context) {
	queue, times, err := r.RTC.GetQueue()
	r.Writer.Records <- times

	if err != nil {
		log.Error().Err(err).Msg("error getting queue to delete all washes queued by routine")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch rtc queue"})
		return
	}

	for _, wash := range queue.Queue.QueueItems {
		if wash.WashPkgNum == 1 {
			times, err := r.RTC.DeleteQueuedCar(wash.WashID)
			r.Writer.Records <- times

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
	if err := r.QueueRoutine.UpdateTime(s); err != nil {
		log.Warn().Err(err).Str("queryTime", s).Msg("error updating queue time, setting it to default")
		r.QueueRoutine.Ticker = time.NewTicker(time.Duration(2 * time.Second))
	}
	r.QueueRoutine.Run(r.RTC, r.Writer)
	log.Info().Str("newTickerTime", s).Msg("successfully updated queue routine's ticker time")
}

func (r *Routines) UpdateMoveTime(c *gin.Context) {
	s := c.Param("seconds")
	if s == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no time span specified"})
		return
	}
	if err := r.MoveRoutine.UpdateTime(s); err != nil {
		log.Warn().Err(err).Str("queryTime", s).Msg("error updating move time, setting it to default")
		r.MoveRoutine.Ticker = time.NewTicker(time.Duration(2 * time.Second))
	}
	go r.MoveRoutine.Run(r.RTC, r.Writer)
	log.Info().Str("newTickerTime", s).Msg("successfully updated move routine's ticker time")
}

func (r *Routines) UpdateGetTime(c *gin.Context) {
	s := c.Param("seconds")
	if s == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no time span specified"})
		return
	}
	if err := r.GetRoutine.UpdateTime(s); err != nil {
		log.Warn().Err(err).Str("queryTime", s).Msg("error updating get time, setting it to default")
		r.GetRoutine.Ticker = time.NewTicker(time.Duration(2 * time.Second))
	}
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

	if err := r.QueueRoutine.UpdateTime(q); err != nil {
		log.Warn().Err(err).Str("queryTime", q).Msg("error updating queue time, setting it to default")
		r.QueueRoutine.Ticker = time.NewTicker(time.Duration(2 * time.Second))
	}

	if err := r.MoveRoutine.UpdateTime(m); err != nil {
		log.Warn().Err(err).Str("queryTime", m).Msg("error updating move time, setting it to default")
		r.MoveRoutine.Ticker = time.NewTicker(time.Duration(2 * time.Second))
	}

	if err := r.GetRoutine.UpdateTime(g); err != nil {
		log.Warn().Err(err).Str("queryTime", g).Msg("error updating get time, setting it to default")
		r.GetRoutine.Ticker = time.NewTicker(time.Duration(2 * time.Second))
	}

	r.RunAll()
}

type QueueRoutine struct {
	Done   chan bool
	Ticker *time.Ticker
}

func CreateQueueRoutine(tickerTime float64, doneChannel chan bool) *QueueRoutine {
	t := fmt.Sprintf("%fs", tickerTime)
	d, err := time.ParseDuration(t)
	if err != nil {
		log.Error().Err(err).Float64("tickerTime", tickerTime).Str("convertedTime", t).Msg("error converting queue car time string to time.duration; forcing ticker duration to be default")
		d = 2 * time.Second
	}
	return &QueueRoutine{
		Done:   doneChannel,
		Ticker: time.NewTicker(d),
	}
}

func (q *QueueRoutine) Run(client *RTCClient, writer *Writer) {
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

			washID, records, err := client.QueueWash(req)
			if err != nil {
				log.Warn().Err(err).Msg("unable to queue wash in queue routine")
				writer.Records <- records
				continue
			}

			records, err = client.DeleteQueuedCar(washID)
			if err != nil {
				log.Warn().Err(err).Msg("unable to delete queued wash in queue routine")
			}
			writer.Records <- records
		}
	}
}

func (q *QueueRoutine) UpdateTime(tickerTime string) error {
	log.Info().Str("newTickerTime", tickerTime).Msg("updating queue routine's ticker time")
	q.Done <- true

	t, err := strconv.ParseFloat(tickerTime, 32)
	if err != nil {
		log.Error().Err(err).Str("updatedTime", tickerTime).Msg("error parsing string to float")
		return err
	}

	var durationString string
	if t < 1 {
		if t < 0.001 {
			durationString = fmt.Sprintf("%fns", t)
		} else if t < 0.01 {
			durationString = fmt.Sprintf("%fus", t)
		} else {
			durationString = fmt.Sprintf("%fms", t)
		}
	} else {
		durationString = fmt.Sprintf("%fs", t)
	}
	d, err := time.ParseDuration(durationString)
	if err != nil {
		log.Error().Err(err).Str("tickerTime", tickerTime).Msg("error converting queue car time string to time.duration; forcing ticker duration to be default")
		d = 2 * time.Second
	}
	q.Ticker = time.NewTicker(d)

	return nil
}

type GetRoutine struct {
	Done   chan bool
	Ticker *time.Ticker
}

func CreateGetRoutine(tickerTime float64, doneChannel chan bool) *GetRoutine {
	t := fmt.Sprintf("%fs", tickerTime)
	d, err := time.ParseDuration(t)
	if err != nil {
		log.Error().Err(err).Float64("tickerTime", tickerTime).Str("convertedTime", t).Msg("error converting get queue time string to time.duration; forcing ticker duration to be default")
		d = 4 * time.Second
	}
	return &GetRoutine{
		Done:   doneChannel,
		Ticker: time.NewTicker(d),
	}
}

func (g *GetRoutine) Run(client *RTCClient, writer *Writer) {
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
			writer.Records <- records
		}
	}
}

func (g *GetRoutine) UpdateTime(tickerTime string) error {
	log.Info().Str("newTickerTime", tickerTime).Msg("updating get routine's ticker time")
	g.Done <- true

	t, err := strconv.ParseFloat(tickerTime, 32)
	if err != nil {
		log.Error().Err(err).Str("updatedTime", tickerTime).Msg("error parsing string to float")
		return err
	}

	var durationString string
	if t < 1 {
		if t < 0.001 {
			durationString = fmt.Sprintf("%fns", t)
		} else if t < 0.01 {
			durationString = fmt.Sprintf("%fus", t)
		} else {
			durationString = fmt.Sprintf("%fms", t)
		}
	} else {
		durationString = fmt.Sprintf("%fs", t)
	}
	d, err := time.ParseDuration(durationString)
	if err != nil {
		log.Error().Err(err).Str("tickerTime", tickerTime).Msg("error converting queue car time string to time.duration; forcing ticker duration to be default")
		d = 2 * time.Second
	}
	g.Ticker = time.NewTicker(d)

	return nil
}

type MoveRoutine struct {
	Done   chan bool
	Ticker *time.Ticker
}

func CreateMoveRoutine(tickerTime float64, doneChannel chan bool) *MoveRoutine {
	t := fmt.Sprintf("%fs", tickerTime)
	d, err := time.ParseDuration(t)
	if err != nil {
		log.Error().Err(err).Float64("tickerTime", tickerTime).Str("convertedTime", t).Msg("error converting move car time string to time.duration; forcing ticker duration to be default")
		d = 6 * time.Second
	}
	return &MoveRoutine{
		Done:   doneChannel,
		Ticker: time.NewTicker(d),
	}
}

func (m *MoveRoutine) Run(client *RTCClient, writer *Writer) {
	for {
		select {
		case <-m.Done:
			log.Info().Msg("move routine received done signal")
			return
		case <-m.Ticker.C:
			req := WashRequest{
				LaneID:      "4",
				OrderID:     "LOAD-TESTING",
				VehicleID:   "NO-VALID-ID",
				WashPackage: 1,
			}

			washID, records, err := client.QueueWash(req)
			if err != nil {
				log.Warn().Err(err).Msg("unable to queue new wash to rTC, not attempting move")
				writer.Records <- records
				continue
			}
			writer.Records <- records

			queue, records, err := client.GetQueue()
			if err != nil {
				log.Warn().Err(err).Msg("error getting queue from rTC, not attempting move")
				writer.Records <- records
				continue
			} else if queue == nil {
				log.Warn().Err(err).Msg("no queue received from rTC, continuing")
				continue
			}
			writer.Records <- records

			numWashes := len(queue.Queue.QueueItems)
			source := rand.NewSource(time.Now().UnixNano())
			r := rand.New(source)
			before := r.Intn(numWashes)
			p := MoveWashReqParams{
				WashID:   washID,
				ToBefore: before,
			}
			_, records, err = client.MoveWash(p)
			if err != nil {
				log.Warn().Err(err).Int("toBefore", before).Msg("error moving wash 1 to before wash")
			}
			writer.Records <- records

			records, err = client.DeleteQueuedCar(washID)
			if err != nil {
				log.Warn().Err(err).Int("washID", washID).Msg("error deleting queued car from rTC")
			}
			writer.Records <- records
		}
	}
}

func (m *MoveRoutine) UpdateTime(tickerTime string) error {
	log.Info().Str("newTickerTime", tickerTime).Msg("updating move routine's ticker time")
	m.Done <- true

	t, err := strconv.ParseFloat(tickerTime, 32)
	if err != nil {
		log.Error().Err(err).Str("updatedTime", tickerTime).Msg("error parsing string to float")
		return err
	}

	var durationString string
	if t < 1 {
		if t < 0.001 {
			durationString = fmt.Sprintf("%fns", t)
		} else if t < 0.01 {
			durationString = fmt.Sprintf("%fus", t)
		} else {
			durationString = fmt.Sprintf("%fms", t)
		}
	} else {
		durationString = fmt.Sprintf("%fs", t)
	}
	d, err := time.ParseDuration(durationString)
	if err != nil {
		log.Error().Err(err).Str("tickerTime", tickerTime).Msg("error converting queue car time string to time.duration; forcing ticker duration to be default")
		d = 2 * time.Second
	}
	m.Ticker = time.NewTicker(d)

	return nil
}

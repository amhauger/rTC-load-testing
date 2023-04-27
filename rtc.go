package main

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

var getQueueXML = "<src><getQueue/></src>"

type WashRequest struct {
	LaneID      string `json:"laneId"`
	OrderID     string `json:"orderId"`
	VehicleID   string `json:"vehicleId"`
	VehicleKey  string `json:"vehicleKey"`
	WashPackage int    `json:"package"`
}

type AddQueueRequest struct {
	XMLName    xml.Name `xml:"src"`
	WashPkgNum int      `xml:"addTail>washPkgNum"`
}

type AddQueueResponse struct {
	XMLName xml.Name `xml:"tc"`
	WashID  int      `xml:"carAdded>id"`
}

func (r *RTCClient) BuildAddTailXML(washPackage int) (string, error) {
	washRequest := AddQueueRequest{
		WashPkgNum: washPackage,
	}

	enc, err := xml.Marshal(washRequest)
	if err != nil {
		return "", errors.Wrapf(err, "unable to marshal")
	}
	return string(enc), nil
}

func (r *RTCClient) ParseRTCAddQueueResponse(message string) (*AddQueueResponse, error) {
	readBytes := []byte(message)
	var wash AddQueueResponse
	convertErr := xml.Unmarshal(readBytes, &wash)
	if convertErr != nil {
		return nil, convertErr
	}

	return &wash, nil
}

func (r *RTCClient) QueueWash(washRequest WashRequest) ([]string, error) {
	record := []string{"QUEUE"}
	queueXML, xmlErr := r.BuildAddTailXML(1)
	if xmlErr != nil {
		log.Error().Err(xmlErr).Msg("error building xml to queue wash")
		record = append(record, time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), "true", xmlErr.Error())
		return record, xmlErr
	}

	log.Info().Str("method", "QueueWash").Str("xml", queueXML).Msg("successfully created queue XML")

	client, connectErr := r.StartConn()
	if connectErr != nil {
		record = append(record, time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), "true", connectErr.Error())
		return record, connectErr
	}
	defer client.Close()
	// connect time
	record = append(record, time.Now().String())

	r.WriteToRTC(client, queueXML)
	// init request time
	record = append(record, time.Now().String())

	_, readErr := r.ReadFromServer(client)
	if readErr != nil {
		record = append(record, time.Time{}.String(), time.Time{}.String(), "true", readErr.Error())
		return record, readErr
	}
	// retrieve request time
	record = append(record, time.Now().String())

	closeErr := client.Close()
	if closeErr != nil {
		log.Error().Err(closeErr).Msg("error closing connection to rTC when queueing wash")
		err := client.SetDeadline(time.Now())
		if err != nil {
			log.Info().Err(err).Msg("error setting deadline when force closing rtc connection")
		}
		time.Sleep(5 * time.Second)

		closeErr = client.Close()
		if closeErr != nil {
			record = append(record, time.Time{}.String(), "true", closeErr.Error())
			log.Error().Err(closeErr).Msg("error forcefully closing connection to rTC")
			return record, closeErr
		}
	}

	record = append(record, time.Now().String(), "false", "")
	return record, nil
}

// MoveWashReqParams is used for taking the params in JSON form, without requiring
// Swagger users to delete the XMLName field each time they want to use it
type MoveWashReqParams struct {
	WashID   int `json:"washId"`
	ToBefore int `json:"toBefore"`
}

type MoveWashRequest struct {
	XMLName  xml.Name `xml:"src"`
	WashID   int      `xml:"move>id"`
	ToBefore int      `xml:"move>before"`
}

func (r *RTCClient) BuildMoveXML(washID int, toBefore int) (string, error) {
	MoveRequest := MoveWashRequest{
		WashID:   washID,
		ToBefore: toBefore,
	}
	enc, err := xml.Marshal(MoveRequest)
	if err != nil {
		return "", errors.Wrapf(err, "Unable to marshal")
	}
	return string(enc), nil
}

func (r *RTCClient) MoveWash(moveRequest MoveWashReqParams) (*GetQueueResponse, []string, error) {
	record := []string{"MOVE"}
	moveXML, xmlErr := r.BuildMoveXML(moveRequest.WashID, moveRequest.ToBefore)
	if xmlErr != nil {
		log.Error().Err(xmlErr).Int("washID", moveRequest.WashID).Int("moveToBefore", moveRequest.ToBefore).Msg("error creating XML to move wash in rTC")
		record = append(record, time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), "true", xmlErr.Error())
		return nil, record, xmlErr
	}

	log.Info().Int("washID", moveRequest.WashID).Int("moveToBefore", moveRequest.ToBefore).Msg("successfully created move XmL")

	client, connectErr := r.StartConn()
	if connectErr != nil {
		record = append(record, time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), "true", connectErr.Error())
		return nil, record, connectErr
	}
	defer client.Close()
	// connect time
	record = append(record, time.Now().String())

	r.WriteToRTC(client, moveXML)
	// init request time
	record = append(record, time.Now().String())

	readMessage, readErr := r.ReadFromServer(client)
	if readErr != nil {
		log.Error().Err(readErr).Int("washID", moveRequest.WashID).Int("moveToBefore", moveRequest.ToBefore).Msg("error reading move request from rTC")
		record = append(record, time.Time{}.String(), time.Time{}.String(), "true", readErr.Error())
		return nil, record, readErr
	}
	// retrieve request time
	record = append(record, time.Now().String())

	closeErr := client.Close()
	if closeErr != nil {
		log.Error().Err(closeErr).Msg("error closing connection to rTC when moving wash")
		err := client.SetDeadline(time.Now())
		if err != nil {
			log.Info().Err(err).Msg("error setting deadline when force closing rtc connection")
		}
		time.Sleep(5 * time.Second)

		closeErr = client.Close()
		if closeErr != nil {
			record = append(record, time.Time{}.String(), "true", closeErr.Error())
			log.Error().Err(closeErr).Msg("error forcefully closing connection to rTC")
			return nil, record, closeErr
		}
	}
	// close time
	record = append(record, time.Now().String(), "false", "")

	resp, err := r.ParseRTCGetQueueResponse(*readMessage)
	return resp, record, err
}

type DeleteWashRequest struct {
	XMLName xml.Name `xml:"src"`
	WashID  int      `xml:"delete>id"`
}

func (r *RTCClient) BuildDeleteXML(washID int) (string, error) {
	DeleteRequest := DeleteWashRequest{
		WashID: washID,
	}

	enc, err := xml.Marshal(DeleteRequest)
	if err != nil {
		return "", errors.Wrap(err, "unable to marshal to XML")
	}

	return string(enc), nil
}

func (r *RTCClient) DeleteQueuedCar(washID int) ([]string, error) {
	record := []string{"DELETE"}
	deleteXML, xmlErr := r.BuildDeleteXML(washID)
	if xmlErr != nil {
		log.Error().Err(xmlErr).Int("washID", washID).Msg("error creating XML to delete wash from rTC")
		record = append(record, time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), "true", xmlErr.Error())
		return record, xmlErr
	}

	log.Info().Str("method", "DeleteWash").Str("xml", deleteXML).Msg("successfully created XML")

	client, connectErr := r.StartConn()
	if connectErr != nil {
		record = append(record, time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), "true", xmlErr.Error())
		return record, connectErr
	}
	defer client.Close()
	// connect time
	record = append(record, time.Now().String())

	r.WriteToRTC(client, deleteXML)
	// init request time
	record = append(record, time.Now().String(), time.Now().String())

	closeErr := client.Close()
	if closeErr != nil {
		log.Error().Err(closeErr).Int("washID", washID).Msg("error closing connection to rTC when deleting from queue")
		err := client.SetDeadline(time.Now())
		if err != nil {
			log.Info().Err(err).Int("washID", washID).Msg("error setting deadline when force closing connection")
		}
		time.Sleep(5 * time.Second)

		closeErr = client.Close()
		if closeErr != nil {
			record = append(record, time.Time{}.String(), "true", closeErr.Error())
			log.Error().Err(closeErr).Msg("error forcefully closing connection to rTC")
			return record, closeErr
		}
	}
	record = append(record, time.Now().String(), "false", "")

	return record, nil
}

type GetQueueResponse struct {
	XMLName xml.Name  `xml:"tc"`
	Queue   WashQueue `xml:"queue"`
}

type WashQueue struct {
	QueueItems []WashQueueItem `xml:"car"`
}

type WashQueueItem struct {
	WashID     int    `xml:"id"`
	State      string `xml:"state"`
	Position   int    `xml:"position"`
	WashPkgNum int    `xml:"washPkgNum"`
}

func (r *RTCClient) ParseRTCGetQueueResponse(message string) (*GetQueueResponse, error) {
	readBytes := []byte(message)

	var wash GetQueueResponse
	convertErr := xml.Unmarshal(readBytes, &wash)
	if convertErr != nil {
		return nil, convertErr
	}

	return &wash, nil
}

func (r *RTCClient) GetQueue() (*GetQueueResponse, []string, error) {
	record := []string{"GET"}
	client, connectErr := r.StartConn()
	if connectErr != nil {
		record = append(record, time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), time.Time{}.String(), "true", connectErr.Error())
		return nil, record, connectErr
	}
	defer client.Close()
	// connection time
	record = append(record, time.Now().String())

	r.WriteToRTC(client, getQueueXML)
	// initialize request time
	record = append(record, time.Now().String())

	readMessage, readErr := r.ReadFromServer(client)
	if readErr != nil {
		record = append(record, time.Time{}.String(), time.Time{}.String(), "true", readErr.Error())
		return nil, record, readErr
	}
	// retrieval time
	record = append(record, time.Now().String())

	closeErr := client.Close()
	if closeErr != nil {
		err := client.SetDeadline(time.Now())
		if err != nil {
			log.Info().Err(err).Msg("error setting deadline when force closing connection")
		}
		time.Sleep(5 * time.Second)

		closeErr = client.Close()
		if closeErr != nil {
			record = append(record, time.Time{}.String(), "true", closeErr.Error())
			log.Err(closeErr).Msg("error forcefully closing connection")
			return nil, record, closeErr
		}
	}
	// close time
	record = append(record, time.Now().String(), "false", "")
	message, err := r.ParseRTCGetQueueResponse(*readMessage)
	return message, record, err
}

type RTCClient struct {
	Host string
	Port int
}

func CreateRTCClient(host string, port int) *RTCClient {
	return &RTCClient{
		Host: host,
		Port: port,
	}
}

func (r *RTCClient) StartConn() (net.Conn, error) {
	client, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", r.Host, r.Port), 3000*time.Millisecond)
	if err != nil {
		return nil, err
	}
	log.Debug().Str("host", r.Host).Int("port", r.Port).Msg("connection opened on port")

	err = client.SetDeadline(time.Now().Add(1500 * time.Millisecond))
	if err != nil {
		log.Error().Err(err).Int("millisecondDeadline", 1500).Msg("error setting read/write deadlines for I/O ops")
	}

	return client, nil
}

func (r *RTCClient) WriteToRTC(client net.Conn, xml string) {
	fmt.Fprint(client, xml)
}

func (r *RTCClient) ReadFromServer(client net.Conn) (*string, error) {
	err := client.SetDeadline(time.Now().Add(3000 * time.Millisecond))
	if err != nil {
		log.Error().Err(err).Msg("error setting read deadline in ReadFromServer()")
	}
	rtcMessage, messageErr := bufio.NewReader(client).ReadString('\n')
	if messageErr != nil && messageErr != io.EOF {
		log.Error().Err(messageErr).Msg("error reading string retrieved from rTC")
		return nil, err
	}

	rtcMessage = strings.TrimSpace(rtcMessage)
	return &rtcMessage, nil
}

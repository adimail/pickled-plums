package main

import (
	"encoding/json"
	"io"
	"math/rand/v2"
	"os"
	"time"

	"github.com/gin-gonic/gin"
)

type SensorData struct {
	Name  string `json: "Name"`
	Value int32  `json: "Value"`
}

type SensorStroke struct {
	Sensors []SensorData `json:"sensors"`
	TS      float64      `json: "ts"`
}

type SensorList struct {
	Sensors []string `json: "sensors"`
}

func generateSensorValues() SensorStroke {
	jsonFile, err := os.Open("./sensors.json")
	if err != nil {
		panic(err)
	}
	plan, _ := io.ReadAll(jsonFile)
	var sensorList SensorList
	err = json.Unmarshal(plan, &sensorList)
	if err != nil {
		panic(err)
	}
	var sensorStroke SensorStroke
	for _, sensorName := range sensorList.Sensors {
		sensorData := SensorData{
			Name:  sensorName,
			Value: rand.Int32N(101),
		}
		sensorStroke.Sensors = append(sensorStroke.Sensors, sensorData)
	}
	sensorStroke.TS = float64(time.Now().Unix())
	return sensorStroke
}

func main() {
	r := gin.Default()
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	r.GET("/sensors", func(c *gin.Context) {
		sensorStroke := generateSensorValues()
		c.JSON(200, gin.H{"data": sensorStroke})
	})
	r.Run()
}

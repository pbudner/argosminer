package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/pbudner/argosminer/stores"
	"github.com/pbudner/argosminer/utils"
)

func RegisterApiHandlers(g *echo.Group, sbarStore *stores.SbarStore, eventStore *stores.EventStore, eventSampler *utils.EventSampler) {
	v1 := g.Group("/v1")
	v1.GET("/", func(c echo.Context) error {
		return c.JSON(http.StatusOK, JSON{
			"message": "Hello, world! Welcome to ArgosMiner!",
			"version": "0.1",
		})
	})

	v1.GET("/events/last/:count", func(c echo.Context) error {
		counter := 10
		i, err := strconv.Atoi(c.Param("count"))
		if err == nil {
			if i < 0 {
				counter = 10
			} else if i > 50 {
				counter = 50
			} else {
				counter = i
			}
		}

		events, err := eventStore.GetLast(counter)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, JSON{
				"error": err.Error(),
			})
		}

		return c.JSON(http.StatusOK, JSON{
			"events": events,
			"count":  len(events),
		})
	})

	v1.GET("/events/frequency", func(c echo.Context) error {
		counter, err := eventStore.GetBinCount()
		if err != nil {
			return c.JSON(http.StatusInternalServerError, JSON{
				"error": err.Error(),
			})
		}

		dailyCounter := make(map[string]uint64)
		for k, v := range counter {
			trimmedKey := fmt.Sprintf("%s-%s-%s", k[0:4], k[4:6], k[6:8])
			_, ok := dailyCounter[trimmedKey]
			if !ok {
				dailyCounter[trimmedKey] = v
			} else {
				dailyCounter[trimmedKey] += v
			}
		}

		return c.JSON(http.StatusOK, JSON{
			"frequency": dailyCounter,
		})
	})

	v1.GET("/statistics", func(c echo.Context) error {
		counter := eventStore.GetCount()
		activityCount := sbarStore.CountActivities()
		dfRelationCount := sbarStore.CountDfRelations()
		return c.JSON(http.StatusOK, JSON{
			"event_count":       counter,
			"activity_count":    activityCount,
			"df_relation_count": dfRelationCount,
			"events_per_second": eventSampler.GetSample(),
		})
	})

	v1.GET("/events/activities", func(c echo.Context) error {
		v := sbarStore.GetActivities()
		return c.JSON(http.StatusOK, JSON{
			"activities": v,
			"count":      len(v),
		})
	})

	v1.GET("/events/dfrelations", func(c echo.Context) error {
		v := sbarStore.GetDfRelations()
		return c.JSON(200, JSON{
			"dfrelations": v,
			"count":       len(v),
		})
	})
}

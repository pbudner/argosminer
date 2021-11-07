package api

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/labstack/echo/v4"
	"github.com/pbudner/argosminer/stores"
	"github.com/pbudner/argosminer/utils"
	"gonum.org/v1/gonum/graph/multi"
	"gonum.org/v1/gonum/graph/topo"
)

func RegisterApiHandlers(g *echo.Group, version, gitCommit string, sbarStore *stores.SbarStore, eventStore *stores.EventStore, eventSampler *utils.EventSampler) {
	v1 := g.Group("/v1")
	v1.GET("/", func(c echo.Context) error {
		return c.JSON(http.StatusOK, JSON{
			"message": "Hello, world! Welcome to ArgosMiner API!",
			"version": version,
			"build":   gitCommit[:6],
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
			trimmedKey := fmt.Sprintf("%s/%s/%s", k[0:4], k[4:6], k[6:8])
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

	type Edge struct {
		From   string `json:"from"`
		To     string `json:"to"`
		Weight int64  `json:"weight,omitempty"`
	}
	type ProcessResult struct {
		ID      uint64   `json:"id"`
		Actions []string `json:"actions"`
		Edges   []Edge   `json:"edges"`
	}

	v1.GET("/activities/frequency", func(c echo.Context) error {
		urlValues := c.Request().URL.Query()
		activities := urlValues["name"]
		result, err := sbarStore.DailyCountOfActivities(activities)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, JSON{
				"error": err.Error(),
			})
		}

		return c.JSON(200, JSON{
			"activities": result,
		})
	})

	v1.GET("/processes", func(c echo.Context) error {
		actionMap := make(map[int64]string)
		g := multi.NewUndirectedGraph()
		relations := sbarStore.GetDfRelations()
		for _, relation := range relations {
			if relation.From != "" {
				from := xxhash.Sum64String(relation.From)
				to := xxhash.Sum64String(relation.To)
				actionMap[int64(from)] = relation.From
				actionMap[int64(to)] = relation.To
				g.SetLine(g.NewLine(multi.Node(from), multi.Node(to)))
			}
		}

		processes := make([]ProcessResult, 0)
		connectedComponents := topo.ConnectedComponents(g)
		for _, nodes := range connectedComponents {
			processActions := ProcessResult{
				Actions: make([]string, 0),
				Edges:   make([]Edge, 0),
			}

			nodeIdList := make(map[int64]bool)

			for _, node := range nodes {
				nodeIdList[node.ID()] = true
				processActions.Actions = append(processActions.Actions, actionMap[node.ID()])
			}
			sort.Strings(processActions.Actions)

			edges := g.Edges()
			for edges.Reset(); edges.Next(); {
				e := edges.Edge()
				if nodeIdList[e.From().ID()] || nodeIdList[e.To().ID()] {
					processActions.Edges = append(processActions.Edges, Edge{
						From: actionMap[e.From().ID()],
						To:   actionMap[e.To().ID()],
					})
				}
			}

			processActions.ID = xxhash.Sum64String(strings.Join(processActions.Actions, ";;"))
			processes = append(processes, processActions)
		}

		return c.JSON(200, JSON{
			"processes": processes,
			"count":     len(processes),
		})
	})
}

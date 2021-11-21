package api

import (
	"fmt"
	"net/http"
	url "net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	b64 "encoding/base64"

	"github.com/cespare/xxhash/v2"
	"github.com/labstack/echo/v4"
	"github.com/pbudner/argosminer/stores"
	"github.com/pbudner/argosminer/utils"
	log "github.com/sirupsen/logrus"
	"gonum.org/v1/gonum/graph/multi"
	"gonum.org/v1/gonum/graph/topo"
)

func RegisterApiHandlers(g *echo.Group, version, gitCommit string, sbarStore *stores.SbarStore, eventStore *stores.EventStore, eventSampler *utils.EventSampler) {
	v1 := g.Group("/v1")
	v1.GET("/", func(c echo.Context) error {
		var commitPrefix string
		if len(gitCommit) > 6 {
			commitPrefix = gitCommit[:6]
		} else {
			commitPrefix = gitCommit
		}
		return c.JSON(http.StatusOK, JSON{
			"message": "Hello, world! Welcome to ArgosMiner API!",
			"version": version,
			"build":   commitPrefix,
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

	v1.GET("/dfrelations", func(c echo.Context) error {
		v := sbarStore.GetDfRelations()
		return c.JSON(200, JSON{
			"dfrelations": v,
			"count":       len(v),
		})
	})

	type Action struct {
		Name   string `json:"name"`
		Degree uint64 `json:"degree,omitempty"`
	}
	type Edge struct {
		From   string  `json:"from"`
		To     string  `json:"to"`
		Weight float64 `json:"weight,omitempty"`
	}
	type ProcessResult struct {
		ID      uint64   `json:"id"`
		Actions []Action `json:"actions"`
		Edges   []Edge   `json:"edges"`
	}

	v1.GET("/activities/frequency", func(c echo.Context) error {
		urlValues := c.Request().URL.Query()
		activities := urlValues["name"]
		for i, v := range activities {
			vDec, err := decodeString(v)
			if err != nil {
				log.Error(err)
				return c.JSON(http.StatusInternalServerError, JSON{
					"error": err.Error(),
				})
			}

			activities[i] = vDec
		}

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

	v1.GET("/dfrelations/timewindow", func(c echo.Context) error {
		urlValues := c.Request().URL.Query()
		startUnix, err := strconv.Atoi(urlValues["start"][0])
		if err != nil {
			return c.JSON(http.StatusInternalServerError, JSON{
				"error": err.Error(),
			})
		}
		start := time.UnixMilli(int64(startUnix))
		endUnix, err := strconv.Atoi(urlValues["end"][0])
		if err != nil {
			return c.JSON(http.StatusInternalServerError, JSON{
				"error": err.Error(),
			})
		}
		end := time.UnixMilli(int64(endUnix))
		encodedFromRelations := urlValues["from"]
		encodedToRelations := urlValues["to"]
		if len(encodedFromRelations) != len(encodedToRelations) {
			return c.JSON(http.StatusBadRequest, JSON{
				"error": "Not the same amount of 'from' and 'to' queries",
			})
		}
		relations := make([][]string, 0)
		for i, encodedFromRelation := range encodedFromRelations {
			from, err := decodeString(encodedFromRelation)
			if err != nil {
				log.Error(err)
				return c.JSON(http.StatusInternalServerError, JSON{
					"error": err.Error(),
				})
			}
			to, err := decodeString(encodedToRelations[i])
			if err != nil {
				log.Error(err)
				return c.JSON(http.StatusInternalServerError, JSON{
					"error": err.Error(),
				})
			}
			relations = append(relations, []string{from, to})
		}
		edges, err := sbarStore.GetDfRelationsWithinTimewindow(relations, start, end)
		if err != nil {
			log.Error(err)
			return c.JSON(http.StatusInternalServerError, JSON{
				"error": err.Error(),
			})
		}

		nodes := make(map[string]struct{})
		for _, edge := range edges {
			nodes[edge.From] = struct{}{}
			nodes[edge.To] = struct{}{}
		}

		uniqueNodes := make([]string, 0, len(nodes))
		for k := range nodes {
			uniqueNodes = append(uniqueNodes, k)
		}

		return c.JSON(200, JSON{
			"nodes": uniqueNodes,
			"edges": edges,
		})
	})

	v1.GET("/processes", func(c echo.Context) error {
		actionMap := make(map[int64]string)
		actionDegreeMap := make(map[int64]uint64)
		g := multi.NewWeightedUndirectedGraph()
		relations := sbarStore.GetDfRelations()
		for _, relation := range relations {
			if relation.From != "" {
				from := xxhash.Sum64String(relation.From)
				to := xxhash.Sum64String(relation.To)
				actionMap[int64(from)] = relation.From
				actionMap[int64(to)] = relation.To
				actionDegreeMap[int64(from)] += relation.Weight
				actionDegreeMap[int64(to)] += relation.Weight
				g.SetWeightedLine(g.NewWeightedLine(multi.Node(from), multi.Node(to), float64(relation.Weight)))
			}
		}

		processes := make([]ProcessResult, 0)
		connectedComponents := topo.ConnectedComponents(g)
		for _, nodes := range connectedComponents {
			processActions := ProcessResult{
				Actions: make([]Action, 0),
				Edges:   make([]Edge, 0),
			}

			nodeIdList := make(map[int64]bool)

			for _, node := range nodes {
				nodeIdList[node.ID()] = true
				processActions.Actions = append(processActions.Actions, Action{Name: actionMap[node.ID()], Degree: actionDegreeMap[node.ID()]})
			}

			actions := make([]string, 0)
			for _, action := range processActions.Actions {
				actions = append(actions, action.Name)
			}

			sort.Strings(actions)
			edges := g.WeightedEdges()
			for edges.Reset(); edges.Next(); {
				e := edges.WeightedEdge()
				if nodeIdList[e.From().ID()] || nodeIdList[e.To().ID()] {
					processActions.Edges = append(processActions.Edges, Edge{
						From:   actionMap[e.From().ID()],
						To:     actionMap[e.To().ID()],
						Weight: e.Weight(),
					})
				}
			}

			processActions.ID = xxhash.Sum64String(strings.Join(actions, ";;"))
			processes = append(processes, processActions)
		}

		return c.JSON(200, JSON{
			"processes": processes,
			"count":     len(processes),
		})
	})
}

func decodeString(v string) (string, error) {
	vDecQueryEncoded, err := b64.StdEncoding.DecodeString(v)
	if err != nil {
		return "", err
	}

	vDec, err := url.QueryUnescape(string(vDecQueryEncoded))
	if err != nil {
		return "", err
	}

	return vDec, nil
}

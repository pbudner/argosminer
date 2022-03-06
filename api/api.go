package api

import (
	"fmt"
	"net/http"
	url "net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	b64 "encoding/base64"

	"github.com/cespare/xxhash/v2"
	"github.com/labstack/echo/v4"
	"github.com/pbudner/argosminer/config"
	"github.com/pbudner/argosminer/stores"
	"github.com/pbudner/argosminer/utils"
	"go.uber.org/zap"
	"gonum.org/v1/gonum/graph/multi"
	"gonum.org/v1/gonum/graph/topo"
)

func RegisterApiHandlers(g *echo.Group, cfg *config.Config, version, gitCommit string) {
	log := zap.L().Sugar()
	v1 := g.Group("/v1")

	welcomeHandlerFunc := func(c echo.Context) error {
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
	}

	v1.GET("", welcomeHandlerFunc)
	v1.GET("/", welcomeHandlerFunc)

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

		eventStore := stores.GetEventStore()
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
		eventStore := stores.GetEventStore()
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
		eventStore := stores.GetEventStore()
		sbarStore := stores.GetSbarStore()
		counter := eventStore.GetCount()
		activityCount := sbarStore.CountActivities()
		dfRelationCount := sbarStore.CountDfRelations()
		return c.JSON(http.StatusOK, JSON{
			"event_count":       counter,
			"activity_count":    activityCount,
			"df_relation_count": dfRelationCount,
			"events_per_second": utils.GetEventSampler().GetSample(),
		})
	})

	v1.GET("/events/activities", func(c echo.Context) error {
		v := stores.GetSbarStore().GetActivities()
		return c.JSON(http.StatusOK, JSON{
			"activities": v,
			"count":      len(v),
		})
	})

	v1.GET("/dfrelations", func(c echo.Context) error {
		v := stores.GetSbarStore().GetDfRelations()
		return c.JSON(200, JSON{
			"dfrelations": v,
			"count":       len(v),
		})
	})

	type Action struct {
		Name   string `json:"name"`
		Degree uint64 `json:"degree"`
	}
	type Edge struct {
		From   string  `json:"from"`
		To     string  `json:"to"`
		Weight float64 `json:"weight"`
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
				log.Errorw("an unexpected error occurred when decoding strings.", "error", err)
				return c.JSON(http.StatusInternalServerError, JSON{
					"error": err.Error(),
				})
			}

			activities[i] = vDec
		}

		result, err := stores.GetSbarStore().DailyCountOfActivities(activities)
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
		edges, err := stores.GetSbarStore().GetDfRelationsWithinTimewindow(relations, start, end)
		if err != nil {
			log.Error(err)
			return c.JSON(http.StatusInternalServerError, JSON{
				"error": err.Error(),
			})
		}

		nodeWeight := make(map[string]uint64)
		nodes := make(map[string]struct{})
		for _, edge := range edges {
			nodes[edge.From] = struct{}{}
			nodes[edge.To] = struct{}{}
			nodeWeight[edge.To] += edge.Weight
		}

		uniqueNodes := make([]Action, 0, len(nodes))
		for k := range nodes {
			uniqueNodes = append(uniqueNodes, Action{
				Name:   k,
				Degree: nodeWeight[k],
			})
		}

		return c.JSON(200, JSON{
			"nodes": uniqueNodes,
			"edges": edges,
		})
	})

	v1.GET("/processes/names", func(c echo.Context) error {
		// TODO
		return nil
	})

	v1.GET("/processes", func(c echo.Context) error {
		ignoreActionReExprs := make([]*regexp.Regexp, 0)
		removeActionReExprs := make([]*regexp.Regexp, 0)
		for _, expr := range cfg.IgnoreActivities {
			if expr.Remove {
				removeActionReExprs = append(removeActionReExprs, regexp.MustCompile(expr.Name))
			} else {
				ignoreActionReExprs = append(ignoreActionReExprs, regexp.MustCompile(expr.Name))
			}
		}

		actionMap := make(map[int64]string)
		actionDegreeMap := make(map[int64]uint64)
		g := multi.NewWeightedUndirectedGraph()
		dg := multi.NewWeightedDirectedGraph()
		relations := stores.GetSbarStore().GetDfRelations()
		for _, relation := range relations {
			ignoreFromActivity := false
			removeFromActivity := false
			ignoreToActivity := false
			removeToActivity := false
			// completely ignore certain activites
			for _, re := range removeActionReExprs {
				if re.MatchString(relation.From) {
					removeFromActivity = true
				}
				if re.MatchString(relation.To) {
					removeToActivity = true
				}
				if removeFromActivity && removeToActivity {
					break
				}
			}

			if !removeFromActivity && !removeToActivity {
				// ignore certain activites only for creating the holistic process map
				for _, re := range ignoreActionReExprs {
					if re.MatchString(relation.From) {
						ignoreFromActivity = true
					}
					if re.MatchString(relation.To) {
						ignoreToActivity = true
					}
					if ignoreFromActivity && ignoreToActivity {
						break
					}
				}
			}

			if removeFromActivity || removeToActivity {
				log.Debugw("Remove a relation for the generation of the holistic graph", "from", relation.From, "to", relation.To)
			}

			if ignoreFromActivity || ignoreToActivity {
				log.Debugw("Ignored a relation for the generation of the holistic graph", "from", relation.From, "to", relation.To)
			}

			from := xxhash.Sum64String(relation.From)
			if !removeFromActivity {
				actionMap[int64(from)] = relation.From
			}

			to := xxhash.Sum64String(relation.To)
			if !removeToActivity {
				actionMap[int64(to)] = relation.To
				actionDegreeMap[int64(to)] += relation.Weight
			}

			// completely remove a directly-follows relation, as one of the activities should be removed
			if removeFromActivity || removeToActivity {
				continue
			}

			if !ignoreFromActivity && !ignoreToActivity && relation.From != "" {
				g.SetWeightedLine(g.NewWeightedLine(multi.Node(from), multi.Node(to), float64(relation.Weight)))
			}

			dg.SetWeightedLine(dg.NewWeightedLine(multi.Node(from), multi.Node(to), float64(relation.Weight)))
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
				if actionMap[node.ID()] == "" {
					continue
				}
				nodeIdList[node.ID()] = true
				processActions.Actions = append(processActions.Actions, Action{Name: actionMap[node.ID()], Degree: actionDegreeMap[node.ID()]})
			}

			actions := make([]string, 0)
			for _, action := range processActions.Actions {
				actions = append(actions, action.Name)
			}

			sort.Strings(actions)
			edges := dg.WeightedEdges()
			postAddedNodeIdList := make(map[int64]bool)

			for edges.Reset(); edges.Next(); {
				e := edges.WeightedEdge()
				fromId := e.From().ID()

				toId := e.To().ID()
				if nodeIdList[fromId] || nodeIdList[toId] {
					processActions.Edges = append(processActions.Edges, Edge{
						From:   actionMap[fromId],
						To:     actionMap[toId],
						Weight: e.Weight(),
					})

					// add action if it was not in there before
					if !nodeIdList[fromId] && !postAddedNodeIdList[fromId] {
						if actionMap[fromId] == "" {
							continue
						}
						postAddedNodeIdList[fromId] = true
						processActions.Actions = append(processActions.Actions, Action{Name: actionMap[fromId], Degree: actionDegreeMap[fromId]})
					}
					if !nodeIdList[toId] && !postAddedNodeIdList[toId] {
						if actionMap[toId] == "" {
							continue
						}
						postAddedNodeIdList[toId] = true
						processActions.Actions = append(processActions.Actions, Action{Name: actionMap[toId], Degree: actionDegreeMap[toId]})
					}
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

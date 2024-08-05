package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type MicroService struct {
	HTTPClient httpDoer
	SNSClient  snsPublisher
}

type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type snsPublisher interface {
	Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
}

type JsonFeed struct {
	Items []JsonFeedItem `json:"items"`
}

type JsonFeedItem struct {
	Url  string `json:"url"`
	Date string `json:"date_published"`
}

func (s *MicroService) GetFeed(ctx context.Context, url string) (*JsonFeed, error) {
	timeout, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()

	req, err := http.NewRequestWithContext(timeout, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", "Spirit Riot Poller (+https://github.com/tkrehbiel/spiritriot-poller-service)")
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var feed JsonFeed
	if err := json.Unmarshal(body, &feed); err != nil {
		return nil, err
	}

	return &feed, nil
}

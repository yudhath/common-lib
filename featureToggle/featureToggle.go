package featuretoggle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
)

var bucketName string

func SetBucketName(name string) {
	bucketName = name
}

type FeatureToggleService struct {}

func getFeatureToggleConfig(s3Client *s3.Client, featureName string) (*FeatureToggleConfig, error) {
	requestInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(featureName + ".json"),
	}

	result, err := s3Client.GetObject(context.TODO(), requestInput)
	if err != nil {
		log.Error("ERROR ", err)
	}
	defer func() {
		if result != nil {
			result.Body.Close()
		}
	}()

	var featureToggleConfig FeatureToggleConfig
	if result != nil {
		body1, err := ioutil.ReadAll(result.Body)
		if err != nil {
			log.Error(err)
		}
		bodyString1 := fmt.Sprintf("%s", body1)
		log.Info("Json String : " + bodyString1)
		decoder := json.NewDecoder(strings.NewReader(bodyString1))
		err = decoder.Decode(&featureToggleConfig)
		if err != nil {
			log.Error("Failed when decode json to struct due to ", err)
		}

		log.Info("Here is the feature toggle struct : ", featureToggleConfig)
	}
	return &featureToggleConfig, err
}

func (fts *FeatureToggleService) UpsertFeatureToggleConfig(s3Client *s3.Client, featureToggleConfig *FeatureToggleConfig) (bool, error) {
	env := os.Getenv("APP_ENV")
	prefix := strings.ToLower(env) + "/"

	if featureToggleConfig != nil || featureToggleConfig == (&FeatureToggleConfig{}) {
		return false, errors.New("feature toggle struct must not empty/nil")
	}

	if featureToggleConfig.Name == "" {
		return false, errors.New("feature toggle name must not empty")
	}

	if featureToggleConfig.Percentage < 0 || featureToggleConfig.Percentage > 100 {
		return false, errors.New("feature toggle percentage value must greater than 0 and less than 100")
	}

	json, err := json.Marshal(featureToggleConfig)
	if err != nil {
		log.Error("Failed when try to marshal config due to ", err)
		return false, err
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(prefix + featureToggleConfig.Name + ".json"),
		Body:   manager.ReadSeekCloser(bytes.NewReader(json)),
	}

	resp, err := s3Client.PutObject(context.TODO(), input)
	if err != nil {
		log.Error("Failed when upload json due to ", err)
		return false, nil
	}

	log.Info("Upsert feature toggle was success with response from AWS : ", resp)
	return true, err
}

func (fts *FeatureToggleService) IsEnabled(s3Client *s3.Client, featureName string) bool {
	featureToggleConfig, err := getFeatureToggleConfig(s3Client, featureName)
	if err != nil {
		log.Error("Failed when get feature toggle due to ", err)
		return false
	}
	return featureToggleConfig != (&FeatureToggleConfig{}) && featureToggleConfig.IsEnabled && isEnabledPartially(featureToggleConfig.Percentage)
}

func isEnabledPartially(percentage int) bool {
	source := rand.NewSource(time.Now().UnixNano())
	randomizer := rand.New(source)

	return randomizer.Intn(100) < percentage
}

func GetJsonFromS3(s3Client *s3.Client, T interface{}, key string) error {
	env := os.Getenv("APP_ENV")
	prefix := strings.ToLower(env) + "/"
	requestInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(prefix + key),
	}

	resp, err := s3Client.GetObject(context.TODO(), requestInput)
	if err != nil {
		return err
	}
	return json.NewDecoder(resp.Body).Decode(T)
}

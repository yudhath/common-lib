package featuretoggle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
)

var bucketName string
var appEnv string

func SetBucketName(value string) {
	bucketName = value
}

func SetAppEnv(value string) {
	appEnv = value
}

func IsEnabled(s3Client *s3.Client, featureName string) bool {
	err := validateMandatoryVariable()
	if err != nil {
		log.Info("Error when invoke IsEnabled() method for featureName ", featureName, " due to ", err)
		return false
	}

	featureToggleConfig, err := getFeatureToggleConfig(s3Client, featureName)
	if err != nil {
		return false
	}
	
	isEnabled := featureToggleConfig != (&FeatureToggleConfig{}) && featureToggleConfig.IsEnabled && isEnabledPartially(featureToggleConfig.Percentage)
	return isEnabled
}

func UpsertFeatureToggleConfig(s3Client *s3.Client, featureToggleConfig *FeatureToggleConfig) (bool, error) {
	prefix := strings.ToLower(appEnv) + "/"

	err := validateMandatoryVariable()
	if err != nil {
		return false, err
	}

	if featureToggleConfig == nil || featureToggleConfig == (&FeatureToggleConfig{}) {
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
		log.Error("Failed when try to marshal feature toggle config due to ", err)
		return false, err
	}

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(prefix + featureToggleConfig.Name + ".json"),
		Body:   manager.ReadSeekCloser(bytes.NewReader(json)),
	}

	resp, err := s3Client.PutObject(context.TODO(), input)
	if err != nil {
		log.Error("Failed when upsert feature toggle config due to ", err)
		return false, err
	}

	log.Info("Upsert feature toggle config was success with response from AWS : ", resp)
	return true, err
}

func DeleteFeatureToggleConfig(s3Client *s3.Client, featureName string) (bool, error) {
	prefix := strings.ToLower(appEnv) + "/"

	err := validateMandatoryVariable()
	if err != nil {
		return false, err
	}

	if featureName == "" {
		return false, errors.New("featureName must not empty")
	}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key: aws.String(prefix + featureName + ".json"),
	}

	resp, err := s3Client.DeleteObject(context.TODO(), input)
	if err != nil {
		log.Error("Failed when delete feature toggle config due to ", err)
		return false, err
	}
	log.Info("Delete feature toggle config was success with response from AWS : ", resp)
	return true, nil
}

func getFeatureToggleConfig(s3Client *s3.Client, featureName string) (*FeatureToggleConfig, error) {
	prefix := strings.ToLower(appEnv) + "/"

	requestInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(prefix + featureName + ".json"),
	}

	result, err := s3Client.GetObject(context.TODO(), requestInput)
	if err != nil {
		log.Error("Error when try to get object from s3 due to ", err)
	}
	defer func() {
		if result != nil {
			result.Body.Close()
		}
	}()

	var featureToggleConfig FeatureToggleConfig
	if result != nil {
		body, err := ioutil.ReadAll(result.Body)
		if err != nil {
			log.Error("Error while read result body from s3 due to ", err)
		}

		bodyString := string(body)
		err = json.NewDecoder(strings.NewReader(bodyString)).Decode(&featureToggleConfig)
		if err != nil {
			log.Error("Failed when decode json to struct due to ", err)
		}
	}
	return &featureToggleConfig, err
}

func isEnabledPartially(percentage int) bool {
	source := rand.NewSource(time.Now().UnixNano())
	randomizer := rand.New(source)

	return randomizer.Intn(100) < percentage
}

func validateMandatoryVariable() error {
	if bucketName == "" {
		return errors.New("bucket name was empty, you must set s3 bucketname using SetBucketName() method before you use any method featuretoggle package")
	}

	if appEnv == "" {
		return errors.New("application environment was empty, you must set application envinronment using SetAppEnv() method before you use any method from featuretoggle package")
	}

	return nil
}

package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type EmailConfigs struct {
	BaseURL         string
	Endpoint        string
	EmailTemplateID string
	APIKey          string
}

// Represents the main request body structure for sending Email Notifications with GC Notify
type EmailRequestBody struct {
	EmailAddress    string               `json:"email_address"`
	TemplateID      string               `json:"template_id"`
	Personalisation EmailPersonalisation `json:"personalisation"`
}

// Represents the variables used in the email template when calling GC Notify
type EmailPersonalisation struct {
	Name         string `json:"name"`
	VolumeName   string `json:"volume_name"`
	DaysLeft     string `json:"days_left"`
	DeletionDate string `json:"deletion_date"`
}

// Based on the given configs, sends a POST request to an email service to send an email
func SendEmail(client *http.Client, conf EmailConfigs, emailAddress string, personal EmailPersonalisation) error {
	// Request Body
	reqBody, err := json.Marshal(
		EmailRequestBody{
			EmailAddress:    emailAddress,
			TemplateID:      conf.EmailTemplateID,
			Personalisation: personal,
		},
	)
	if err != nil {
		return fmt.Errorf("Failed to create email request body: %v", err)
	}

	// Create the http request and add the required headers
	request, err := http.NewRequest("POST", conf.BaseURL+conf.Endpoint, bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("Failed to create email POST request: %v", err)
	}

	request.Header.Add("Authorization", "ApiKey-v1 "+conf.APIKey)
	request.Header.Add("Content-Type", "application/json")

	// Send Request
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("Error while sending email request: %v", err)
	}

	defer response.Body.Close()

	// we read the body mainly for debugging purposes in the error log messages
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("Error while reading body of email response: %v", err)
	}

	// Expected response is 201
	if response.StatusCode != 201 {
		return fmt.Errorf("Received unexpected response code %d for email request: %s", response.StatusCode, body)
	}

	return nil
}

// Returns a struct containing the variables that will be used in the email template
func GetEmailPersonalisation(namespace string, pvc string, daysLeft float64) EmailPersonalisation {
	// Calculate the expected DeletionDate
	now := time.Now()
	futureTime := now.Add(time.Duration(daysLeft) * 24 * time.Hour)

	personal := EmailPersonalisation{
		Name:         namespace,
		VolumeName:   pvc,
		DaysLeft:     fmt.Sprintf("%f", daysLeft),
		DeletionDate: futureTime.Format(time.UnixDate),
	}

	return personal
}

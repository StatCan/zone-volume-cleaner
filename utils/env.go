package utils

import (
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
)

// Source - https://stackoverflow.com/a/40326580
// Posted by janos, modified by community. See post 'Timeline' for change history
// Retrieved 2026-10-06, License - CC BY-SA 3.0

// Function to return the value of an environment variable if present;
// returns the given fallback value if it is not present
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// Returns an environment variable converted to int,
// or the given fallback if it is not present
func GetIntEnv(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		intValue, err := strconv.Atoi(value)
		if err != nil {
			log.Fatalf("Error while converting string Env value to int: %s", err.Error())
		}

		return intValue
	}
	return fallback
}

// Converts the given string into an array of ints sorted in descending order
// Each value represents an amount of days for when to send notifications to users with volumes approching being stale
// Ex. A value of "[5,1]" means to notify users 5 days away and 1 day away from their volume's cleanup date
func ParseNotificationTimes(str string) []int {
	var intSlice []int

	if str == "" {
		return []int{}
	}

	// try to convert each value to an int, error out if failed
	parsedString := strings.Split(str, ",")
	for _, val := range parsedString {
		converted, err := strconv.Atoi(val)
		if err != nil {
			log.Fatalf("Failed to parse notification time: %s", err)
		}
		intSlice = append(intSlice, converted)
	}

	// sort final slice of ints
	slices.Sort(intSlice)
	slices.Reverse(intSlice)

	return intSlice
}

package broker

import (
	"testing"
	"time"
)

func TestCalculateRetryBackoff(t *testing.T) {
	b := &Broker{
		retryBackoffBase:       1 * time.Second,
		retryBackoffMultiplier: 2.0,
		retryBackoffMax:        60 * time.Second,
	}
	
	tests := []struct {
		name         string
		base         time.Duration
		multiplier   float64
		max          time.Duration
		retryCount   int
		expectedMin  time.Duration
		expectedMax  time.Duration
	}{
		{
			name:         "Default exponential backoff - retry 1",
			base:         1 * time.Second,
			multiplier:   2.0,
			max:          60 * time.Second,
			retryCount:   1,
			expectedMin:  2 * time.Second,
			expectedMax:  2 * time.Second,
		},
		{
			name:         "Default exponential backoff - retry 2",
			base:         1 * time.Second,
			multiplier:   2.0,
			max:          60 * time.Second,
			retryCount:   2,
			expectedMin:  4 * time.Second,
			expectedMax:  4 * time.Second,
		},
		{
			name:         "Default exponential backoff - retry 3",
			base:         1 * time.Second,
			multiplier:   2.0,
			max:          60 * time.Second,
			retryCount:   3,
			expectedMin:  8 * time.Second,
			expectedMax:  8 * time.Second,
		},
		{
			name:         "Capped at max",
			base:         1 * time.Second,
			multiplier:   2.0,
			max:          10 * time.Second,
			retryCount:   10,
			expectedMin:  10 * time.Second,
			expectedMax:  10 * time.Second,
		},
		{
			name:         "Linear backoff",
			base:         5 * time.Second,
			multiplier:   1.0,
			max:          60 * time.Second,
			retryCount:   5,
			expectedMin:  5 * time.Second,
			expectedMax:  5 * time.Second,
		},
		{
			name:         "Fast retry for development",
			base:         100 * time.Millisecond,
			multiplier:   1.5,
			max:          10 * time.Second,
			retryCount:   3,
			expectedMin:  330 * time.Millisecond,
			expectedMax:  340 * time.Millisecond,
		},
		{
			name:         "Zero retry count returns base",
			base:         2 * time.Second,
			multiplier:   2.0,
			max:          60 * time.Second,
			retryCount:   0,
			expectedMin:  2 * time.Second,
			expectedMax:  2 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b.SetRetryBackoff(tt.base, tt.multiplier, tt.max)
			delay := b.calculateRetryBackoff(tt.retryCount)
			
			if delay < tt.expectedMin || delay > tt.expectedMax {
				t.Errorf("calculateRetryBackoff() = %v, want between %v and %v",
					delay, tt.expectedMin, tt.expectedMax)
			}
			t.Logf("Retry %d: delay = %v (expected: %v - %v)",
				tt.retryCount, delay, tt.expectedMin, tt.expectedMax)
		})
	}
}

func TestRetryBackoffProgression(t *testing.T) {
	b := &Broker{
		retryBackoffBase:       1 * time.Second,
		retryBackoffMultiplier: 2.0,
		retryBackoffMax:        60 * time.Second,
	}
	b.SetRetryBackoff(1*time.Second, 2.0, 60*time.Second)

	expectedDelays := []time.Duration{
		1 * time.Second,  // retry 0
		2 * time.Second,  // retry 1
		4 * time.Second,  // retry 2
		8 * time.Second,  // retry 3
		16 * time.Second, // retry 4
		32 * time.Second, // retry 5
		60 * time.Second, // retry 6 (capped)
		60 * time.Second, // retry 7 (capped)
	}

	for i, expected := range expectedDelays {
		actual := b.calculateRetryBackoff(i)
		if actual != expected {
			t.Errorf("Retry %d: got %v, want %v", i, actual, expected)
		}
		t.Logf("Retry %d: %v", i, actual)
	}
}

func TestRetryBackoffConcurrency(t *testing.T) {
	b := &Broker{
		retryBackoffBase:       1 * time.Second,
		retryBackoffMultiplier: 2.0,
		retryBackoffMax:        60 * time.Second,
	}
	b.SetRetryBackoff(1*time.Second, 2.0, 60*time.Second)

	// Test concurrent access to calculateRetryBackoff
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(retryCount int) {
			for j := 0; j < 100; j++ {
				delay := b.calculateRetryBackoff(retryCount)
				if delay < 0 {
					t.Errorf("Negative delay: %v", delay)
				}
			}
			done <- true
		}(i % 5)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

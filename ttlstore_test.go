package ttlstore

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	janitor := NewDeleter("To be seen companions", nil, 10*time.Minute, 21*time.Second,
		10000, 200000, nil)
	s1 := New(janitor.Set)
	janitor.DeleteFunc = s1.Delete
	go janitor.Start()

	s1.Set("my key", 1)

}

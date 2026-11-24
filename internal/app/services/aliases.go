package services

import (
	"time"

	"crep/internal/app/ports"
	"crep/internal/domain"
)

type (
	timeT    = time.Time
	pageJobs = ports.Page[domain.Job]
)

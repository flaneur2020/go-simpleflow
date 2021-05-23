package flowserver

type Storage interface {
	PopJob(limit int) []JobID

	Enqueue(buf []byte) (JobID, error)

	Update(jobID JobID, state string, buf []byte) error

	Load(jobID JobID) (string, []byte, error)
}

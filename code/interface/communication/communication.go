package communication

import "io"

func Read(reader io.Reader, buffer []byte) error {
	var total int = 0
	var current int = 0
	var err error = nil

	for total < len(buffer) {
		if current, err = reader.Read(buffer[total:]); err != nil {
			return err
		} else {
			total += current
		}
	}

	return nil
}

func Write(data []byte, length uint64, writer io.Writer) error {
	var n uint64 = 0

	for n < length {
		w, err := writer.Write(data[n:])

		if err != nil {
			return err
		} else {
			n += uint64(w)
		}
	}

	return nil
}

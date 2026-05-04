package access

func stringToByteSlice(ss []string) [][]byte {
	result := make([][]byte, len(ss))
	for i, s := range ss {
		result[i] = []byte(s)
	}
	return result
}

func byteSliceToString(bb [][]byte) []string {
	result := make([]string, len(bb))
	for i, b := range bb {
		result[i] = string(b)
	}
	return result
}

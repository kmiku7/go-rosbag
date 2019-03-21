package rosbag

type IdGenerator struct {
	idsMap map[string]int
}

func NewIdGenerator() *IdGenerator {
	return &IdGenerator{
		idsMap: make(map[string]int),
	}
}

func (i *IdGenerator) GetId(key string) int {
	if _, has := i.idsMap[key]; !has {
		i.idsMap[key] = len(i.idsMap)
	}
	return i.idsMap[key]
}

func (i *IdGenerator) GetUint32Id(key string) uint32 {
	return uint32(i.GetId(key))
}

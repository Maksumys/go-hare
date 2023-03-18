package middlewares

type HeadersCarrier map[string]interface{}

func (a HeadersCarrier) Get(key string) string {
	v, ok := a[key]
	if !ok {
		return ""
	}
	return v.(string)
}

func (a HeadersCarrier) Set(key string, value string) {
	a[key] = value
}

func (a HeadersCarrier) Keys() []string {
	keys := make([]string, 0, len(a))

	for key := range a {
		keys = append(keys, key)
	}

	return keys
}

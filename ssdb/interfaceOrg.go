package ssdb

import (
	"fmt"
	"log"
	"math"
	"reflect"
	"strings"
)

//Setx : setx
func (c *Client) SetX(key string, val string, ttl int) (interface{}, error) {
	params := []interface{}{key, val, ttl}
	return c.processCmd("setx", params)
}

//Scan : scan
func (c *Client) Scan(start string, end string, limit int) (interface{}, error) {
	params := []interface{}{start, end, limit}
	return c.processCmd("scan", params)
}

func (c *Client) Expire(key string, ttl int) (interface{}, error) {
	params := []interface{}{key, ttl}
	return c.processCmd("expire", params)
}

func (c *Client) KeyTTL(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.processCmd("ttl", params)
}

//set new key if key exists then ignore this operation
func (c *Client) SetNew(key string, val string) (interface{}, error) {
	params := []interface{}{key, val}
	return c.processCmd("setnx", params)
}

//
func (c *Client) GetSet(key string, val string) (interface{}, error) {
	params := []interface{}{key, val}
	return c.processCmd("getset", params)
}

//incr num to exist number value
func (c *Client) Incr(key string, val int) (interface{}, error) {
	params := []interface{}{key, val}
	return c.processCmd("incr", params)
}

func (c *Client) Exists(key string) (interface{}, error) {
	params := []interface{}{key}
	return c.processCmd("exists", params)
}

func (c *Client) HashSet(hash string, key string, val string) (interface{}, error) {
	params := []interface{}{hash, key, val}
	return c.processCmd("hset", params)
}

func (c *Client) HashGet(hash string, key string) (interface{}, error) {
	params := []interface{}{hash, key}
	return c.processCmd("hget", params)
}

func (c *Client) HashDel(hash string, key string) (interface{}, error) {
	params := []interface{}{hash, key}
	return c.processCmd("hdel", params)
}

func (c *Client) HashIncr(hash string, key string, val int) (interface{}, error) {
	params := []interface{}{hash, key, val}
	return c.processCmd("hincr", params)
}

func (c *Client) HashExists(hash string, key string) (interface{}, error) {
	params := []interface{}{hash, key}
	return c.processCmd("hexists", params)
}

func (c *Client) HashSize(hash string) (interface{}, error) {
	params := []interface{}{hash}
	return c.processCmd("hsize", params)
}

//search from start to end hashmap name or haskmap key name,except start word
func (c *Client) HashList(start string, end string, limit int) (interface{}, error) {
	params := []interface{}{start, end, limit}
	return c.processCmd("hlist", params)
}

func (c *Client) HashKeys(hash string, start string, end string, limit int) (interface{}, error) {
	params := []interface{}{hash, start, end, limit}
	return c.processCmd("hkeys", params)
}
func (c *Client) HashKeysAll(hash string) ([]string, error) {
	size, err := c.HashSize(hash)
	if err != nil {
		return nil, err
	}
	log.Printf("DB Hash Size:%d\n", size)
	hashSize := size.(int64)
	page_range := 15
	splitSize := math.Ceil(float64(hashSize) / float64(page_range))
	log.Printf("DB Hash Size:%d hashSize:%d splitSize:%f\n", size, hashSize, splitSize)
	var range_keys []string
	for i := 1; i <= int(splitSize); i++ {
		start := ""
		end := ""
		if len(range_keys) != 0 {
			start = range_keys[len(range_keys)-1]
			end = ""
		}

		val, err := c.HashKeys(hash, start, end, page_range)
		if err != nil {
			log.Println("HashGetAll Error:", err)
			continue
		}
		if val == nil {
			continue
		}
		//log.Println("HashGetAll type:",reflect.TypeOf(val))
		var data []string
		if fmt.Sprintf("%v", reflect.TypeOf(val)) == "string" {
			data = append(data, val.(string))
		} else {
			data = val.([]string)
		}

		if len(data) > 0 {
			range_keys = append(range_keys, data...)
		}

	}
	log.Printf("DB Hash Keys Size:%d\n", len(range_keys))
	return range_keys, nil
}

func (c *Client) HashGetAll(hash string) (map[string]string, error) {
	params := []interface{}{hash}
	val, err := c.processCmd("hgetall", params)
	if err != nil {
		return nil, err
	} else {
		return val.(map[string]string), err
	}
}

func (c *Client) HashGetAllLite(hash string) (map[string]string, error) {
	size, err := c.HashSize(hash)
	if err != nil {
		return nil, err
	}
	//log.Printf("DB Hash Size:%d\n",size)
	hashSize := size.(int64)
	page_range := 20
	splitSize := math.Ceil(float64(hashSize) / float64(page_range))
	//log.Printf("DB Hash Size:%d hashSize:%d splitSize:%f\n",size,hashSize,splitSize)
	var range_keys []string
	GetResult := make(map[string]string)
	for i := 1; i <= int(splitSize); i++ {
		start := ""
		end := ""
		if len(range_keys) != 0 {
			start = range_keys[len(range_keys)-1]
			end = ""
		}

		val, err := c.HashKeys(hash, start, end, page_range)
		if err != nil {
			log.Println("HashGetAll Error:", err)
			continue
		}
		if val == nil {
			continue
		}
		//log.Println("HashGetAll type:",reflect.TypeOf(val))
		var data []string
		if fmt.Sprintf("%v", reflect.TypeOf(val)) == "string" {
			data = append(data, val.(string))
		} else {
			data = val.([]string)
		}
		range_keys = data
		if len(data) > 0 {
			result, err := c.HashMultiGet(hash, data)
			if err != nil {
				log.Println("HashGetAll Error:", err)
			}
			if result == nil {
				continue
			}
			for k, v := range result {
				GetResult[k] = v
			}
		}

	}

	return GetResult, nil
}

func (c *Client) HashScan(hash string, start string, end string, limit int) (map[string]string, error) {
	params := []interface{}{hash, start, end, limit}
	val, err := c.processCmd("hscan", params)
	if err != nil {
		return nil, err
	} else {
		return val.(map[string]string), err
	}
}

func (c *Client) HashRScan(hash string, start string, end string, limit int) (map[string]string, error) {
	params := []interface{}{hash, start, end, limit}
	val, err := c.processCmd("hrscan", params)
	if err != nil {
		return nil, err
	} else {
		return val.(map[string]string), err
	}
}

func (c *Client) HashMultiSet(hash string, data map[string]string) (interface{}, error) {
	params := []interface{}{hash}
	for k, v := range data {
		params = append(params, k)
		params = append(params, v)
	}
	return c.processCmd("multi_hset", params)
}

func (c *Client) HashMultiGet(hash string, keys []string) (map[string]string, error) {
	params := []interface{}{hash}
	for _, v := range keys {
		params = append(params, v)
	}
	val, err := c.processCmd("multi_hget", params)
	if err != nil {
		return nil, err
	} else {
		return val.(map[string]string), err
	}
}

func (c *Client) HashMultiDel(hash string, keys []string) (interface{}, error) {
	params := []interface{}{hash}
	for _, v := range keys {
		params = append(params, v)
	}
	return c.processCmd("multi_hdel", params)
}

func (c *Client) HashClear(hash string) (interface{}, error) {
	params := []interface{}{hash}
	return c.processCmd("hclear", params)
}

func (c *Client) MultiMode(args [][]interface{}) ([]string, error) {

	for _, v := range args {
		err := c.send(v)
		if err != nil {
			return nil, err
		}
	}
	var resps []string
	for i := 0; i < len(args); i++ {
		resp, err := c.recv()
		if err != nil {

			return nil, err
		}
		resps = append(resps, strings.Join(resp, ","))
	}
	return resps, nil
}

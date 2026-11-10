package sdk

import (
	"sort"

	"github.com/mnemosyne-ai-systems/chorus-sdk-go/routekey"
)

// WithNodeEndpoints 配置数据面节点列表。设置后客户端按 sid 用 rendezvous
// 哈希从列表里选择节点，连接失败时按同一哈希顺序换下一个节点。
//
// 无状态服务端的 ListNodes 只返回虚拟节点 "lb"，因此真实节点列表必须由调用方
// 通过该选项提供。不设置时仍按路由缓存里的 endpoint 直连，行为不变。
func WithNodeEndpoints(endpoints ...string) Option {
	eps := append([]string(nil), endpoints...)
	return func(c *Client) { c.nodeEndpoints = eps }
}

// rendezvousScore 用 routekey.Hash 计算 (sid, endpoint) 的 rendezvous 得分。
//
// FNV-1a 对"公共前缀 + 短后缀"的输入，尾部字节会留下可观测的相关性（实测
// 直接取 Hash(sid+endpoint) 时 3 节点分布是 50%/25%/25%）。这里在 routekey.Hash
// 之后加一次 splitmix64 终混，打散这种相关性，保证增加/删除一个节点只移动
// 约 1/N 的 sid。
func rendezvousScore(sid, endpoint string) uint64 {
	// 用 NUL 分隔，避免 (sid, endpoint) 拼接产生歧义。
	x := routekey.Hash(sid + "\x00" + endpoint)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// orderNodeEndpoints 返回按 rendezvous 哈希得分从高到低排列的节点。
// 得分相同的极端情况用 endpoint 字典序打破平局，保证同一 sid 的选择稳定。
func orderNodeEndpoints(sid string, endpoints []string) []string {
	type scored struct {
		endpoint string
		score    uint64
	}
	list := make([]scored, len(endpoints))
	for i, ep := range endpoints {
		list[i] = scored{endpoint: ep, score: rendezvousScore(sid, ep)}
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].score != list[j].score {
			return list[i].score > list[j].score
		}
		return list[i].endpoint < list[j].endpoint
	})
	out := make([]string, len(list))
	for i := range list {
		out[i] = list[i].endpoint
	}
	return out
}

// dataEndpoints 返回本次请求应尝试的 endpoint 顺序。
func (c *Client) dataEndpoints(sid string, node Node) []string {
	if len(c.nodeEndpoints) == 0 {
		return []string{node.Endpoint}
	}
	return orderNodeEndpoints(sid, c.nodeEndpoints)
}

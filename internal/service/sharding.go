// Copyright 2023 ecodeclub
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

type Dst struct {
	DB string
	// 例如说 local_msg_0, local_msg_1
	Table string
}

// Sharding 如果你有分库分表，那么就需要提供这个配置
type Sharding struct {
	// 返回分库和分表的信息
	// info 是你用来分库分表的信息，这取决于你的业务是如何执行的
	ShardingFunc func(info any) Dst
	// 有效的目标表，例如说在按照日期分库分表之后，
	// 那么老的日期用的表就已经用不上了
	// 定时任务会迭代所有的表，而后找出发送失败的消息，进行补发
	EffectiveTablesFunc func() []Dst
}

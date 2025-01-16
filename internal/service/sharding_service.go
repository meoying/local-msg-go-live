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

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"gorm.io/gorm"
	"time"
)

type ShardingService struct {
	dbs map[string]*gorm.DB

	sharding Sharding
	// 返回 DB
	// 比如说在用户场景下，shardingKey 对应的是你的用户 ID
	// 如果是在订单场景下，对应的是买家 ID
	// 国际业务，用户按照 region + id 来分库分表
	//shardingFunc func(shardingKey any) Dst

	// 本地消息表的所有的库所有的表
	//allTables []Dst

	producer sarama.SyncProducer
}

func NewShardingService(
	dbs map[string]*gorm.DB,
	sharding Sharding,
	producer sarama.SyncProducer) *ShardingService {
	return &ShardingService{
		dbs:      dbs,
		sharding: sharding,
		producer: producer,
	}
}

// StartAsyncTask 补偿任务
// 它不是定时任务
// 今晚：把它扩展到支持分库分表
func (svc *ShardingService) StartAsyncTask() error {
	const limit = 10
	// 一个目标表一个 goroutine
	for _, dst := range svc.sharding.EffectiveTablesFunc() {
		dst := dst
		go func() {
			db, ok := svc.dbs[dst.DB]
			if !ok {
				// 打点日志
				return
			}
			for {
				// time.Minute.Milliseconds() 根据你同步发送消息的一般的时长
				now := time.Now().UnixMilli() - (time.Second * 3).Milliseconds()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				var msgs []Msg
				err := db.WithContext(ctx).
					Table(dst.Table).
					Where("status = ? AND utime < ?", MsgStatusInit, now).
					Offset(0).
					Limit(limit).
					Find(&msgs).Error
				cancel()
				if err != nil {
					// 要不要结束这个异步任务？
					// 可以考虑进一步区分是偶发性错误，还是持续性错误
					continue
				}
				for _, msg := range msgs {
					ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
					err = svc.sendMsg(ctx, db, msg, dst.Table)
					cancel()
					if err != nil {
						// 怎么处理？
						continue
					}
				}
				if len(msgs) == 0 {
					// 没找到
					time.Sleep(time.Second)
				}
			}
		}()

	}
	return nil
}

// 我希望我作为一个业务方，我调用一下这个方法就 OJBK
func (svc *ShardingService) ExecTx(ctx context.Context,
	// 你分库分表啊，你不得告诉我
	shardingKey any,
	biz func(tx *gorm.DB) (Msg, error),
) error {
	// 好问题来了，我怎么拿到它的sharding key
	dst := svc.sharding.ShardingFunc(shardingKey)
	db, ok := svc.dbs[dst.DB]
	if !ok {
		// DB 没有这个 DB
		// 1. shardingFunc 有问题
		// 2. 你的参数有问题
		// 3. 你初始化的时候有问题
		return errors.New("db not exist")
	}
	var msg Msg
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		// 注册成功了
		msg, err = biz(tx)
		if err != nil {
			// 代表的是业务有问题
			return err
		}
		// 在补偿任务 1:1 模型之下，你这边可以用自增主键
		now := time.Now().UnixMilli()
		msg.Ctime = now
		msg.Utime = now
		// 我要把这个 msg 储存好，我要存哪里？
		return tx.Table(dst.Table).Create(&msg).Error
	})
	if err != nil {
		return err
	}
	// 直接发送
	err = svc.sendMsg(ctx, db, msg, dst.Table)
	if err != nil {
		// 站在业务方讲，它的业务是成功了的
		// 打印日志，而后不用管了
		return fmt.Errorf("send msg: %w, cause %w",
			syncSendMsgError, err)
	}
	return nil
}

func (svc *ShardingService) sendMsg(ctx context.Context,
	db *gorm.DB, msg Msg, table string) error {
	_, _, err := svc.producer.SendMessage(&sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.StringEncoder(msg.Content),
	})
	if err != nil {
		return err
	}
	return db.WithContext(ctx).
		Where("id = ?", msg.Id).
		Table(table).
		Updates(map[string]interface{}{
			"status": MsgStatusSuccess,
			"utime":  time.Now().UnixMilli(),
		}).Error
}

// SaveMsg 在你的业务的事务里面调用这个方法
// 害得把事务也给我
func (svc *ShardingService) SaveMsg(tx *gorm.DB, msg Msg) error {
	return tx.Create(&msg).Error
}

//func (svc *ShardingService) UpdateMsg(status int) error {
//	// 更新为成功或者失败
//	// TODO
//	return nil
//}
//
//func (svc *ShardingService) SendMsg(ctx context.Context, msg Msg) error {
//	err := svc.sendMsg(ctx, msg)
//	if err != nil {
//		return err
//	}
//	return svc.UpdateMsg(1)
//}
//
//func (svc *ShardingService) sendMsg(ctx context.Context, msg Msg) error {
//	return nil
//}

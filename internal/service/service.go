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

var (
	syncSendMsgError = errors.New("同步发送消息失败")
)

type Service struct {
	db       *gorm.DB
	producer sarama.SyncProducer
}

// 下周三 20:00 继续
func (svc *Service) StartAsyncTask() error {
	const limit = 10
	for {
		// time.Minute.Milliseconds() 根据你同步发送消息的一般的时长
		now := time.Now().UnixMilli() - (time.Second * 3).Milliseconds()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		var msgs []Msg
		err := svc.db.WithContext(ctx).
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
			err = svc.sendMsg(ctx, msg)
			cancel()
			if err != nil {
				// 怎么处理？
				continue
			}
		}
	}
}

func (svc *Service) sendMsg(ctx context.Context, msg Msg) error {
	_, _, err := svc.producer.SendMessage(&sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.StringEncoder(msg.Content),
	})
	if err != nil {
		return err
	}
	return svc.db.WithContext(ctx).
		Where("id = ?", msg.Id).
		Updates(map[string]interface{}{
			"status": MsgStatusSuccess,
			"utime":  time.Now().UnixMilli(),
		}).Error
}

// 我希望我作为一个业务方，我调用一下这个方法就 OJBK
func (svc *Service) ExecTx(ctx context.Context,
	biz func(tx *gorm.DB) (Msg, error),
) error {
	var msg Msg
	err := svc.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		msg, err = biz(tx)
		if err != nil {
			// 代表的是业务有问题
			return err
		}
		now := time.Now().UnixMilli()
		msg.Ctime = now
		msg.Utime = now
		return tx.Create(&msg).Error
	})
	if err != nil {
		return err
	}
	err = svc.sendMsg(ctx, msg)
	if err != nil {
		// 站在业务方讲，它的业务是成功了的
		// 打印日志，而后不用管了
		return fmt.Errorf("send msg: %w, cause %w",
			syncSendMsgError, err)
	}
	return nil
}

// SaveMsg 在你的业务的事务里面调用这个方法
// 害得把事务也给我
func (svc *Service) SaveMsg(tx *gorm.DB, msg Msg) error {
	return tx.Create(&msg).Error
}

//func (svc *Service) UpdateMsg(status int) error {
//	// 更新为成功或者失败
//	// TODO
//	return nil
//}
//
//func (svc *Service) SendMsg(ctx context.Context, msg Msg) error {
//	err := svc.sendMsg(ctx, msg)
//	if err != nil {
//		return err
//	}
//	return svc.UpdateMsg(1)
//}
//
//func (svc *Service) sendMsg(ctx context.Context, msg Msg) error {
//	return nil
//}

type Msg struct {
	Id int `gorm:"primaryKey,autoIncrement"`
	// 理论上最少字段，就是这两个
	Topic   string
	Content string

	Status int8
	Utime  int64
	Ctime  int64
}

const (
	MsgStatusInit int8 = iota
	MsgStatusSuccess
	MsgStatusFail
)

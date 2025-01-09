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

package local_msg_go_live

import (
	"context"
	"github.com/meoying/local-msg-go-live/internal/service"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
	"testing"
)

type BizTestSuite struct {
	suite.Suite
	db  *gorm.DB
	svc *service.Service
}

func (s *BizTestSuite) SetupSuite() {
	s.svc = &service.Service{}
	go func() {
		s.svc.StartAsyncTask()
	}()
}
func (s *BizTestSuite) TestBizExecTx() {
	s.svc.ExecTx(context.Background(), func() {
		// 这就是它的业务操作
		// 剩下不需要操心了
	})
}

func (s *BizTestSuite) TestBiz() {
	var msg service.Msg

	// 这个是你的业务
	err := s.db.Transaction(func(tx *gorm.DB) error {
		// 前面就是你的业务
		// 第一个关键点，你要在本地事务里面，先把这个消息存好
		// 此时是未发送状态
		msg = service.Msg{
			Topic:   "test_topic",
			Content: "测试一下内容",
		}
		err := s.svc.SaveMsg(tx, msg)
		return err
	})

	// 这样行不行？

	// 现在的目标就是确保 msg 一定能够发出去
	// 直接发送 + 补偿任务
	// 如果我这里发送失败，我就会走补偿任务

	// 我需要有一个东西把这个 msg 存起来，而后我才能说在直接发送失败的时候
	// 找到这个 msg，并且发送出去
	err = s.svc.SendMsg(context.Background(), msg)
	if err == nil {
		// 更新成已发送状态/发送成功状态
		s.svc.UpdateMsg(0)
	}
}

func TestBiz(t *testing.T) {
	// 基本业务流程：

}

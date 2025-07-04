/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package leastactive

import (
	"fmt"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/protocol/base"
	"dubbo.apache.org/dubbo-go/v3/protocol/invocation"
)

func TestLeastActiveSelect(t *testing.T) {
	loadBalance := newLeastActiveLoadBalance()

	var invokers []base.Invoker

	url, _ := common.NewURL(fmt.Sprintf("dubbo://%s:%d/org.apache.demo.HelloService", constant.LocalHostValue, constant.DefaultPort))
	invokers = append(invokers, base.NewBaseInvoker(url))
	i := loadBalance.Select(invokers, &invocation.RPCInvocation{})
	assert.True(t, i.GetURL().URLEqual(url))

	for i := 1; i < 10; i++ {
		url, _ := common.NewURL(fmt.Sprintf("dubbo://192.168.1.%v:20000/org.apache.demo.HelloService", i))
		invokers = append(invokers, base.NewBaseInvoker(url))
	}
	loadBalance.Select(invokers, &invocation.RPCInvocation{})
}

func TestLeastActiveByWeight(t *testing.T) {
	loadBalance := newLeastActiveLoadBalance()

	var invokers []base.Invoker
	loop := 3
	for i := 1; i <= loop; i++ {
		url, _ := common.NewURL(fmt.Sprintf("test%v://192.168.1.%v:20000/org.apache.demo.HelloService?weight=%v", i, i, i))
		invokers = append(invokers, base.NewBaseInvoker(url))
	}

	inv := invocation.NewRPCInvocationWithOptions(invocation.WithMethodName("test"))
	base.BeginCount(invokers[2].GetURL(), inv.MethodName())

	loop = 10000

	var (
		firstCount  int
		secondCount int
	)

	for i := 1; i <= loop; i++ {
		invoker := loadBalance.Select(invokers, inv)
		if invoker.GetURL().Protocol == "test1" {
			firstCount++
		} else if invoker.GetURL().Protocol == "test2" {
			secondCount++
		}
	}

	assert.Equal(t, firstCount+secondCount, loop)
}

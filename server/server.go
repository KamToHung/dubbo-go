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

// Package server provides APIs for registering services and starting an RPC server.
package server

import (
	"sort"
	"strconv"
	"sync"
)

import (
	"github.com/dubbogo/gost/log/logger"

	"github.com/pkg/errors"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/dubboutil"
	"dubbo.apache.org/dubbo-go/v3/metadata"
	"dubbo.apache.org/dubbo-go/v3/registry/exposed_tmp"
)

// proServices are for internal services
var internalProServices = make([]*InternalService, 0, 16)
var internalProLock sync.Mutex

type Server struct {
	cfg *ServerOptions

	mu sync.RWMutex
	// key: *ServiceOptions, value: *common.ServiceInfo
	//proServices map[string]common.RPCService
	// change any to *common.ServiceInfo @see config/service.go
	svcOptsMap map[string]*ServiceOptions
	// key is interface name, value is *ServiceOptions
	interfaceNameServices map[string]*ServiceOptions
	// indicate whether the server is already started
	serve bool
}

// ServiceInfo Deprecated： common.ServiceInfo type alias, just for compatible with old generate pb.go file
type ServiceInfo = common.ServiceInfo

// MethodInfo Deprecated： common.MethodInfo type alias， just for compatible with old generate pb.go file
type MethodInfo = common.MethodInfo

type ServiceDefinition struct {
	Handler any
	Info    *common.ServiceInfo
	Opts    []ServiceOption
}

// Register assemble invoker chains like ProviderConfig.Load, init a service per call
func (s *Server) Register(handler any, info *common.ServiceInfo, opts ...ServiceOption) error {
	return s.registerWithMode(handler, info, constant.IDL, opts...)
}

// RegisterService is for new Triple non-idl mode implement.
func (s *Server) RegisterService(handler any, opts ...ServiceOption) error {
	return s.registerWithMode(handler, nil, constant.NONIDL, opts...)
}

// registerWithMode unified service registration logic
func (s *Server) registerWithMode(handler any, info *common.ServiceInfo, idlMode string, opts ...ServiceOption) error {
	baseOpts := []ServiceOption{
		WithIDLMode(idlMode),
	}
	// only need to explicitly set interface in NONIDL mode
	if idlMode == constant.NONIDL {
		baseOpts = append(baseOpts, WithInterface(common.GetReference(handler)))
	}
	baseOpts = append(baseOpts, opts...)
	newSvcOpts, err := s.genSvcOpts(handler, baseOpts...)
	if err != nil {
		return err
	}
	s.registerServiceOptions(newSvcOpts)
	return nil
}

func (s *Server) genSvcOpts(handler any, opts ...ServiceOption) (*ServiceOptions, error) {
	if s.cfg == nil {
		return nil, errors.New("Server has not been initialized, please use NewServer() to create Server")
	}
	var svcOpts []ServiceOption
	appCfg := s.cfg.Application
	proCfg := s.cfg.Provider
	prosCfg := s.cfg.Protocols
	regsCfg := s.cfg.Registries
	// todo(DMwangnima): record the registered service
	newSvcOpts := defaultServiceOptions()
	if appCfg != nil {
		svcOpts = append(svcOpts,
			SetApplication(s.cfg.Application),
		)
	}
	if proCfg != nil {
		svcOpts = append(svcOpts,
			SetProvider(proCfg),
		)
	}
	if prosCfg != nil {
		svcOpts = append(svcOpts,
			SetProtocols(prosCfg),
		)
	}
	if regsCfg != nil {
		svcOpts = append(svcOpts,
			SetRegistries(regsCfg),
		)
	}
	// Get service-level configuration items from provider.services configuration
	if proCfg != nil && proCfg.Services != nil {
		// Get the unique identifier of the handler (the default is the structure name or the alias set during registration)
		interfaceName := common.GetReference(handler)
		newSvcOpts.Id = interfaceName
		// Give priority to accurately finding the service configuration from the configuration based on the reference name (i.e. the handler registration name)
		svcCfg, ok := proCfg.Services[interfaceName]
		if !ok {
			//fallback: traverse matching interface fields
			for _, cfg := range proCfg.Services {
				if cfg.Interface == interfaceName {
					svcCfg = cfg
				}
			}
		}
		// TODO @see server/action.go Export
		//if newSvcOpts.info != nil {
		//	if newSvcOpts.Service.Interface == "" {
		//		newSvcOpts.Service.Interface = newSvcOpts.info.InterfaceName
		//	}
		//	//newSvcOpts.info = info
		//}

		if svcCfg != nil {
			svcOpts = append(svcOpts,
				SetService(svcCfg),
			)
			logger.Infof("Injected options from provider.services for %s", interfaceName)
		} else {
			logger.Warnf("No matching service config found for [%s]", interfaceName)
		}
	}
	// options passed by users have higher priority
	svcOpts = append(svcOpts, opts...)
	if err := newSvcOpts.init(s, svcOpts...); err != nil {
		return nil, err
	}
	newSvcOpts.Implement(handler)
	newSvcOpts.info = enhanceServiceInfo(newSvcOpts.info)
	return newSvcOpts, nil
}

// Add a method with a name of a different first-letter case
// to achieve interoperability with java
// TODO: The method name case sensitivity in Dubbo-java should be addressed.
// We ought to make changes to handle this issue.
func enhanceServiceInfo(info *common.ServiceInfo) *common.ServiceInfo {
	var additionalMethods []common.MethodInfo
	for _, method := range info.Methods {
		newMethod := method
		newMethod.Name = dubboutil.SwapCaseFirstRune(method.Name)
		additionalMethods = append(additionalMethods, newMethod)
	}
	info.Methods = append(info.Methods, additionalMethods...)
	return info
}

// exportServices export services in svcOptsMap
func (s *Server) exportServices() error {
	var err error
	for _, svcOpts := range s.svcOptsMap {
		err := svcOpts.Export()
		if err != nil {
			logger.Errorf("export %s service failed, err: %s", svcOpts.Service.Interface, err)
			err = errors.Wrapf(err, "failed to export service %s", svcOpts.Service.Interface)
			return err
		}
	}
	return err
}

func (s *Server) Serve() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serve {
		return errors.New("server has already been started")
	}
	// prevent multiple calls to Serve
	s.serve = true
	// the registryConfig in ServiceOptions and ServerOptions all need to init a metadataReporter,
	// when ServiceOptions.init() is called we don't know if a new registry config is set in the future use serviceOption
	if err := metadata.InitRegistryMetadataReport(s.cfg.Registries); err != nil {
		return err
	}
	metadataOpts := metadata.NewOptions(
		metadata.WithAppName(s.cfg.Application.Name),
		metadata.WithMetadataType(s.cfg.Application.MetadataType),
		metadata.WithPort(getMetadataPort(s.cfg)),
		metadata.WithMetadataProtocol(s.cfg.Application.MetadataServiceProtocol),
	)
	if err := metadataOpts.Init(); err != nil {
		return err
	}

	if err := s.exportServices(); err != nil {
		return err
	}
	if err := s.exportInternalServices(); err != nil {
		return err
	}
	if err := exposed_tmp.RegisterServiceInstance(); err != nil {
		return err
	}
	select {}
}

func (s *Server) createInternalServiceOptions() *ServiceOptions {
	return &ServiceOptions{
		Application: s.cfg.Application,
		Provider:    s.cfg.Provider,
		Protocols:   s.cfg.Protocols,
		Registries:  s.cfg.Registries,
	}
}

// In order to expose internal services
func (s *Server) exportInternalServices() error {
	cfg := s.createInternalServiceOptions()
	services := make([]*InternalService, 0, len(internalProServices))

	internalProLock.Lock()
	defer internalProLock.Unlock()
	for _, service := range internalProServices {
		if service.Init == nil {
			return errors.New("[internal service]internal service init func is empty, please set the init func correctly")
		}
		sd, ok := service.Init(cfg)
		if !ok {
			logger.Infof("[internal service]%s service will not expose", service.Name)
			continue
		}
		newSvcOpts, err := s.genSvcOpts(sd.Handler, sd.Opts...)
		if err != nil {
			return err
		}
		service.svcOpts = newSvcOpts
		service.info = sd.Info
		services = append(services, service)
	}

	sort.Slice(services, func(i, j int) bool {
		return services[i].Priority < services[j].Priority
	})

	for _, service := range services {
		if service.BeforeExport != nil {
			service.BeforeExport(service.svcOpts)
		}
		err := service.svcOpts.Export()
		if service.AfterExport != nil {
			service.AfterExport(service.svcOpts, err)
		}
		if err != nil {
			logger.Errorf("[internal service]export %s service failed, err: %s", service.Name, err)
			return err
		}
	}

	return nil
}

// InternalService for dubbo internal services
type InternalService struct {
	// This is required
	// internal service name
	Name    string
	svcOpts *ServiceOptions
	info    *common.ServiceInfo
	// This is required
	// This options is service configuration
	// Return serviceDefinition and bool, where bool indicates whether it is exported
	Init func(options *ServiceOptions) (*ServiceDefinition, bool)
	// This options is InternalService.svcOpts itself
	BeforeExport func(options *ServiceOptions)
	// This options is InternalService.svcOpts itself
	AfterExport func(options *ServiceOptions, err error)
	// Priority of service exposure
	// Lower numbers have the higher priority
	// The default priority is 0
	// The metadata service is exposed at the end
	// If you have no requirements for the order of service exposure, you can use the default priority or not set
	Priority int
}

func getMetadataPort(opts *ServerOptions) int {
	port := opts.Application.MetadataServicePort
	if port == "" {
		protocolConfig, ok := opts.Protocols[constant.DefaultProtocol]
		if ok {
			port = protocolConfig.Port
		}
	}
	if port == "" {
		return 0
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		logger.Error("MetadataService port parse error %v, MetadataService will use random port", err)
		return 0
	}
	return p
}

func NewServer(opts ...ServerOption) (*Server, error) {
	newSrvOpts := defaultServerOptions()
	if err := newSrvOpts.init(opts...); err != nil {
		return nil, err
	}

	srv := &Server{
		cfg:                   newSrvOpts,
		svcOptsMap:            make(map[string]*ServiceOptions),
		interfaceNameServices: make(map[string]*ServiceOptions),
	}
	return srv, nil
}

func SetProviderServices(sd *InternalService) {
	if sd.Name == "" {
		logger.Warnf("[internal service]internal name is empty, please set internal name")
		return
	}
	internalProLock.Lock()
	defer internalProLock.Unlock()
	internalProServices = append(internalProServices, sd)
}

func (s *Server) registerServiceOptions(serviceOptions *ServiceOptions) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logger.Infof("A provider service %s was registered successfully.", serviceOptions.Id)
	s.svcOptsMap[serviceOptions.Id] = serviceOptions
}

// GetServiceOptions retrieves the ServiceOptions for a service by its name/ID
func (s *Server) GetServiceOptions(name string) *ServiceOptions {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.svcOptsMap[name]
}

// GetServiceInfo retrieves the ServiceInfo for a service by its name/ID
// Returns nil if the service is not found or has no ServiceInfo
func (s *Server) GetServiceInfo(name string) *common.ServiceInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if svcOpts, ok := s.svcOptsMap[name]; ok {
		return svcOpts.info
	}
	return nil
}

// GetRPCService retrieves the RPCService implementation for a service by its name/ID
// Returns nil if the service is not found or has no RPCService
func (s *Server) GetRPCService(name string) common.RPCService {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if svcOpts, ok := s.svcOptsMap[name]; ok {
		return svcOpts.rpcService
	}
	return nil
}

// GetServiceOptionsByInterfaceName retrieves the ServiceOptions for a service by its interface name
// Returns nil if no service is found with the given interface name
func (s *Server) GetServiceOptionsByInterfaceName(interfaceName string) *ServiceOptions {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.interfaceNameServices[interfaceName]
}

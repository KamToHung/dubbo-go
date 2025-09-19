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

package config

import (
	"strings"
)

import (
	log "github.com/dubbogo/gost/log/logger"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/rawbytes"

	"github.com/pkg/errors"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common/constant/file"
)

// GetConfigResolver creates and returns a config resolver with placeholder resolution
// Returns error instead of panic for better error handling
func GetConfigResolver(conf *loaderConf) *koanf.Koanf {
	if conf == nil {
		return nil
	}
	if conf.suffix == "" {
		conf.suffix = string(file.YAML)
	}
	if conf.delim == "" {
		conf.delim = "."
	}
	if len(conf.bytes) == 0 {
		panic(errors.New("bytes is nil, please set bytes or file path"))
	}

	k := koanf.New(conf.delim)

	var err error
	switch strings.ToLower(conf.suffix) {
	case "yaml", "yml":
		err = k.Load(rawbytes.Provider(conf.bytes), yaml.Parser())
	case "json":
		err = k.Load(rawbytes.Provider(conf.bytes), json.Parser())
	default:
		err = errors.Errorf("no support %s file suffix", conf.suffix)
	}
	if err != nil {
		panic(err)
	}

	return resolvePlaceholder(k)
}

// resolvePlaceholder replaces ${key:defaultValue} placeholders with actual values
func resolvePlaceholder(resolver *koanf.Koanf) *koanf.Koanf {
	m := make(map[string]any)
	for k, v := range resolver.All() {
		s, ok := v.(string)
		if !ok {
			continue
		}
		placeholderKey, defaultValue := extractPlaceholder(s)
		if placeholderKey == "" {
			continue
		}
		actualValue := resolver.Get(placeholderKey)
		if actualValue == nil {
			actualValue = defaultValue
		}
		m[k] = actualValue
	}
	if len(m) > 0 {
		if err := resolver.Load(confmap.Provider(m, resolver.Delim()), nil); err != nil {
			log.Errorf("failed to resolve placeholders: %v", err)
		}
	}
	return resolver
}

// extractPlaceholder extracts placeholder key and default value from a string like "${key:defaultValue}"
// Returns empty string for key if the input is not a valid placeholder
// e.g. "${config.key:defaultValue}" -> ("config.key", "defaultValue")
func extractPlaceholder(input string) (string, string) {
	s := strings.TrimSpace(input)
	if !strings.HasPrefix(s, file.PlaceholderPrefix) || !strings.HasSuffix(s, file.PlaceholderSuffix) {
		return "", ""
	}
	content := s[len(file.PlaceholderPrefix) : len(s)-len(file.PlaceholderSuffix)]
	if colonIndex := strings.Index(content, ":"); colonIndex >= 0 {
		return strings.TrimSpace(content[:colonIndex]), strings.TrimSpace(content[colonIndex+1:])
	}
	return strings.TrimSpace(content), ""
}

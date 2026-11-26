package limiter

import (
	"net/http"
	"strings"
)

// PathTemplate is a route-shaped key template. Parameter segments may use
// ":name", "{name}", "<name>", or "*".
type PathTemplate = string

// PathPattern is an alternative name for PathTemplate.
type PathPattern = string

// PathTemplateMatcher matches request paths against configured path templates
// without compiling or running regular expressions during request handling.
type PathTemplateMatcher struct {
	root *pathTemplateNode
}

// PathTrie is an alternative name for PathTemplateMatcher.
type PathTrie = PathTemplateMatcher

// PathTemplateTrie is an alternative name for PathTemplateMatcher.
type PathTemplateTrie = PathTemplateMatcher

type pathTemplateNode struct {
	literals map[string]*pathTemplateNode
	param    *pathTemplateNode
	catchAll *pathTemplateNode
	template string
}

type pathSegmentKind int

const (
	pathLiteralSegment pathSegmentKind = iota
	pathParameterSegment
	pathCatchAllSegment
)

// NewPathTemplateMatcher builds an immutable segment tree for the templates.
func NewPathTemplateMatcher(templates ...string) *PathTemplateMatcher {
	matcher := &PathTemplateMatcher{root: &pathTemplateNode{}}
	for _, template := range templates {
		matcher.add(template)
	}
	return matcher
}

// NewPathPatternMatcher is an alias for NewPathTemplateMatcher.
func NewPathPatternMatcher(patterns ...string) *PathTemplateMatcher {
	return NewPathTemplateMatcher(patterns...)
}

// NewPathTrie creates an empty path template trie.
func NewPathTrie(templates ...string) *PathTrie {
	return NewPathTemplateMatcher(templates...)
}

// NewPathTemplateTrie creates an empty path template trie.
func NewPathTemplateTrie(templates ...string) *PathTemplateTrie {
	return NewPathTemplateMatcher(templates...)
}

// Add inserts one template into the matcher.
func (m *PathTemplateMatcher) Add(template string) {
	m.add(template)
}

// Insert inserts one template into the matcher.
func (m *PathTemplateMatcher) Insert(template string) {
	m.add(template)
}

func (m *PathTemplateMatcher) add(template string) {
	if m == nil || m.root == nil {
		return
	}

	node := m.root
	for _, segment := range pathTemplateSegments(template) {
		kind, value := parsePathTemplateSegment(segment)

		switch kind {
		case pathParameterSegment:
			if node.param == nil {
				node.param = &pathTemplateNode{}
			}
			node = node.param
		case pathCatchAllSegment:
			if node.catchAll == nil {
				node.catchAll = &pathTemplateNode{}
			}
			node.catchAll.template = template
			return
		default:
			if node.literals == nil {
				node.literals = make(map[string]*pathTemplateNode)
			}
			child, found := node.literals[value]
			if !found {
				child = &pathTemplateNode{}
				node.literals[value] = child
			}
			node = child
		}
	}

	if node.template == "" {
		node.template = template
	}
}

// Match returns the first matching configured template. Literal segments take
// priority over parameter segments, while still allowing backtracking when a
// more specific branch does not reach a complete match.
func (m *PathTemplateMatcher) Match(path string) (string, bool) {
	if m == nil || m.root == nil {
		return "", false
	}

	return m.root.match(pathTemplateSegments(path), 0)
}

// Normalize returns the matched template or the original path when no template
// matches. Keeping an unmatched path raw prevents unrelated routes from being
// merged into one broad bucket.
func (m *PathTemplateMatcher) Normalize(path string) string {
	if template, found := m.Match(path); found {
		return template
	}
	return path
}

// MatchPath is an alias for Match.
func (m *PathTemplateMatcher) MatchPath(path string) (string, bool) {
	return m.Match(path)
}

// NormalizePath is an alias for Normalize.
func (m *PathTemplateMatcher) NormalizePath(path string) string {
	return m.Normalize(path)
}

func (n *pathTemplateNode) match(segments []string, index int) (string, bool) {
	if index == len(segments) {
		if n.template != "" {
			return n.template, true
		}
		if n.catchAll != nil && n.catchAll.template != "" {
			return n.catchAll.template, true
		}
		return "", false
	}

	segment := segments[index]

	if segment != "" {
		if child := n.literals[segment]; child != nil {
			if template, found := child.match(segments, index+1); found {
				return template, true
			}
		}

		if n.param != nil {
			if template, found := n.param.match(segments, index+1); found {
				return template, true
			}
		}
	} else if child := n.literals[""]; child != nil {
		if template, found := child.match(segments, index+1); found {
			return template, true
		}
	}

	if n.catchAll != nil && n.catchAll.template != "" {
		return n.catchAll.template, true
	}

	return "", false
}

func pathTemplateSegments(path string) []string {
	path = strings.TrimLeft(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

func parsePathTemplateSegment(segment string) (pathSegmentKind, string) {
	switch {
	case segment == "*":
		return pathCatchAllSegment, segment
	case segment == "?":
		return pathParameterSegment, segment
	case strings.HasPrefix(segment, ":") && len(segment) > 1:
		return pathParameterSegment, segment[1:]
	case strings.HasPrefix(segment, "*") && len(segment) > 1:
		return pathCatchAllSegment, segment[1:]
	case strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") && len(segment) >= 2:
		name := strings.TrimSuffix(strings.TrimPrefix(segment, "{"), "}")
		if separator := strings.IndexByte(name, ':'); separator >= 0 {
			name = name[:separator]
		}
		name = strings.TrimPrefix(name, "*")
		if strings.HasSuffix(name, "...") {
			return pathCatchAllSegment, strings.TrimSuffix(name, "...")
		}
		return pathParameterSegment, name
	case strings.HasPrefix(segment, "<") && strings.HasSuffix(segment, ">") && len(segment) >= 2:
		name := strings.TrimSuffix(strings.TrimPrefix(segment, "<"), ">")
		if separator := strings.LastIndexByte(name, ':'); separator >= 0 {
			name = name[separator+1:]
		}
		return pathParameterSegment, name
	case strings.HasPrefix(segment, "[") && strings.HasSuffix(segment, "]") && len(segment) >= 2:
		name := strings.TrimSuffix(strings.TrimPrefix(segment, "["), "]")
		name = strings.TrimPrefix(name, ".")
		name = strings.TrimPrefix(name, ".")
		name = strings.TrimPrefix(name, ".")
		if strings.HasPrefix(name, "...") {
			return pathCatchAllSegment, strings.TrimPrefix(name, "...")
		}
		return pathParameterSegment, name
	default:
		return pathLiteralSegment, segment
	}
}

// SetPathTemplates configures the route templates used to normalize URL paths
// in rate-limit keys.
func (l *Limiter) SetPathTemplates(templates []string) *Limiter {
	copied := append([]string(nil), templates...)

	l.Lock()
	l.pathTemplates = copied
	if len(copied) == 0 {
		l.pathTemplateMatcher.Store(nil)
	} else {
		l.pathTemplateMatcher.Store(NewPathTemplateMatcher(copied...))
	}
	l.Unlock()

	return l
}

// GetPathTemplates returns a copy of the configured route templates.
func (l *Limiter) GetPathTemplates() []string {
	l.RLock()
	defer l.RUnlock()

	return append([]string(nil), l.pathTemplates...)
}

// SetPathTemplate configures the only route template.
func (l *Limiter) SetPathTemplate(template string) *Limiter {
	return l.SetPathTemplates([]string{template})
}

// AddPathTemplate adds one route template without replacing existing templates.
func (l *Limiter) AddPathTemplate(template string) *Limiter {
	l.Lock()
	defer l.Unlock()

	for _, existing := range l.pathTemplates {
		if existing == template {
			return l
		}
	}

	l.pathTemplates = append(l.pathTemplates, template)
	l.pathTemplateMatcher.Store(NewPathTemplateMatcher(l.pathTemplates...))
	return l
}

// AddPathTemplates adds route templates without replacing existing templates.
func (l *Limiter) AddPathTemplates(templates ...string) *Limiter {
	for _, template := range templates {
		l.AddPathTemplate(template)
	}
	return l
}

// GetPathTemplate returns the first configured route template.
func (l *Limiter) GetPathTemplate() string {
	templates := l.GetPathTemplates()
	if len(templates) == 0 {
		return ""
	}
	return templates[0]
}

// NormalizePath normalizes path using the configured route templates.
func (l *Limiter) NormalizePath(path string) string {
	matcher := l.pathTemplateMatcher.Load()
	if matcher == nil {
		return path
	}
	return matcher.Normalize(path)
}

// NormalizePath provides a package-level convenience entry point. It accepts
// path/templates, templates/path, or a Limiter with either a path or request.
func NormalizePath(args ...any) string {
	var configuredLimiter *Limiter
	var requestPath string
	var stringArgs []string
	var templates []string

	for _, arg := range args {
		switch typed := arg.(type) {
		case *Limiter:
			configuredLimiter = typed
		case *http.Request:
			requestPath = typed.URL.Path
		case string:
			stringArgs = append(stringArgs, typed)
		case []string:
			templates = append(templates, typed...)
		case []any:
			for _, value := range typed {
				if text, ok := value.(string); ok {
					templates = append(templates, text)
				}
			}
		}
	}

	if requestPath == "" && len(stringArgs) > 0 {
		requestPath = stringArgs[0]
		stringArgs = stringArgs[1:]
	}
	templates = append(templates, stringArgs...)

	if configuredLimiter != nil {
		return configuredLimiter.NormalizePath(requestPath)
	}

	normalized := NewPathTemplateMatcher(templates...).Normalize(requestPath)
	if normalized != requestPath || len(stringArgs) != 1 {
		return normalized
	}

	reversed := NewPathTemplateMatcher(requestPath).Normalize(stringArgs[0])
	if reversed != stringArgs[0] {
		return reversed
	}

	return normalized
}

// GetNormalizedPath is an alias for NormalizePath.
func (l *Limiter) GetNormalizedPath(path string) string {
	return l.NormalizePath(path)
}

// SetPathPatterns is an alias for SetPathTemplates.
func (l *Limiter) SetPathPatterns(patterns []string) *Limiter {
	return l.SetPathTemplates(patterns)
}

// GetPathPatterns is an alias for GetPathTemplates.
func (l *Limiter) GetPathPatterns() []string {
	return l.GetPathTemplates()
}

// SetPathPattern is an alias for SetPathTemplate.
func (l *Limiter) SetPathPattern(pattern string) *Limiter {
	return l.SetPathTemplate(pattern)
}

// AddPathPattern is an alias for AddPathTemplate.
func (l *Limiter) AddPathPattern(pattern string) *Limiter {
	return l.AddPathTemplate(pattern)
}

// GetPathPattern is an alias for GetPathTemplate.
func (l *Limiter) GetPathPattern() string {
	return l.GetPathTemplate()
}

// SetURLTemplates is an alias for SetPathTemplates.
func (l *Limiter) SetURLTemplates(templates []string) *Limiter {
	return l.SetPathTemplates(templates)
}

// GetURLTemplates is an alias for GetPathTemplates.
func (l *Limiter) GetURLTemplates() []string {
	return l.GetPathTemplates()
}

// SetURLTemplate is an alias for SetPathTemplate.
func (l *Limiter) SetURLTemplate(template string) *Limiter {
	return l.SetPathTemplate(template)
}

// SetRouteTemplates is an alias for SetPathTemplates.
func (l *Limiter) SetRouteTemplates(templates []string) *Limiter {
	return l.SetPathTemplates(templates)
}

// GetRouteTemplates is an alias for GetPathTemplates.
func (l *Limiter) GetRouteTemplates() []string {
	return l.GetPathTemplates()
}

// SetRouteTemplate is an alias for SetPathTemplate.
func (l *Limiter) SetRouteTemplate(template string) *Limiter {
	return l.SetPathTemplate(template)
}

// SetRoutePatterns is an alias for SetPathTemplates.
func (l *Limiter) SetRoutePatterns(patterns []string) *Limiter {
	return l.SetPathTemplates(patterns)
}

// GetRoutePatterns is an alias for GetPathTemplates.
func (l *Limiter) GetRoutePatterns() []string {
	return l.GetPathTemplates()
}

// SetRoutePattern is an alias for SetPathPattern.
func (l *Limiter) SetRoutePattern(pattern string) *Limiter {
	return l.SetPathPattern(pattern)
}

// SetURLPathTemplates is an alias for SetPathTemplates.
func (l *Limiter) SetURLPathTemplates(templates []string) *Limiter {
	return l.SetPathTemplates(templates)
}

// GetURLPathTemplates is an alias for GetPathTemplates.
func (l *Limiter) GetURLPathTemplates() []string {
	return l.GetPathTemplates()
}

// SetURLPathTemplate is an alias for SetPathTemplate.
func (l *Limiter) SetURLPathTemplate(template string) *Limiter {
	return l.SetPathTemplate(template)
}

// SetURLPathPatterns is an alias for SetPathPatterns.
func (l *Limiter) SetURLPathPatterns(patterns []string) *Limiter {
	return l.SetPathPatterns(patterns)
}

// SetURLPathPattern is an alias for SetPathPattern.
func (l *Limiter) SetURLPathPattern(pattern string) *Limiter {
	return l.SetPathPattern(pattern)
}

// SetRequestPathTemplates is an alias for SetPathTemplates.
func (l *Limiter) SetRequestPathTemplates(templates []string) *Limiter {
	return l.SetPathTemplates(templates)
}

// SetNormalizedPathTemplates is an alias for SetPathTemplates.
func (l *Limiter) SetNormalizedPathTemplates(templates []string) *Limiter {
	return l.SetPathTemplates(templates)
}

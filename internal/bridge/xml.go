package bridge

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	xmlTrue  = "true"
	xmlFalse = "false"
	xmlError = "error"
)

// parseXML validates the whole document, not just its first opening tag.
func parseXML(message string) (xml.StartElement, error) {
	const op = "bridge.parseXML"
	d := xml.NewDecoder(strings.NewReader(message))
	var root xml.StartElement
	depth, roots := 0, 0
	for {
		token, err := d.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return root, fmt.Errorf("%s: malformed XML", op)
		}
		switch t := token.(type) {
		case xml.StartElement:
			if depth == 0 {
				roots++
				root = t
			}
			depth++
			if depth > 64 {
				return root, fmt.Errorf("%s: XML nesting limit exceeded", op)
			}
		case xml.EndElement:
			depth--
		case xml.Directive:
			return root, fmt.Errorf("%s: XML directives are not supported", op)
		case xml.CharData:
			if depth == 0 && strings.TrimSpace(string(t)) != "" {
				return root, fmt.Errorf("%s: text outside root", op)
			}
		}
	}
	if roots != 1 || depth != 0 {
		return root, fmt.Errorf("%s: expected one XML root", op)
	}
	return root, nil
}

func commandName(message string) (string, error) {
	const op = "bridge.commandName"
	root, err := parseXML(message)
	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}
	if root.Name.Local != "command" || root.Name.Space != "" {
		return "", fmt.Errorf("%s: expected command root", op)
	}
	for _, attr := range root.Attr {
		if attr.Name.Local == "id" && attr.Name.Space == "" && strings.TrimSpace(attr.Value) != "" {
			return attr.Value, nil
		}
	}
	return "", fmt.Errorf("%s: missing command id", op)
}

func responseKind(message string) (string, string, error) {
	const op = "bridge.responseKind"
	root, err := parseXML(message)
	if err != nil {
		return "", "", fmt.Errorf("%s: %w", op, err)
	}
	if root.Name.Space != "" {
		return "response", "", nil
	}
	switch root.Name.Local {
	case xmlError:
		var response struct {
			Text string `xml:",chardata"`
		}
		if err := xml.Unmarshal([]byte(message), &response); err != nil {
			return "", "", fmt.Errorf("%s: %w", op, err)
		}
		text := strings.TrimSpace(response.Text)
		if text == "" {
			text = "connector returned an unspecified error"
		}
		return "connector_error", text, nil
	case "result":
		for _, attr := range root.Attr {
			if attr.Name.Local == "success" {
				switch attr.Value {
				case xmlTrue:
					return "accepted", "", nil
				case xmlFalse:
					return "rejected", "", nil
				}
			}
		}
		return "", "", fmt.Errorf("%s: result has no valid success attribute", op)
	default:
		return "response", "", nil
	}
}

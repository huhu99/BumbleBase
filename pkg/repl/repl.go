package repl

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	uuid "github.com/google/uuid"
)

// REPL struct.
type REPL struct {
	commands map[string]func(string, *REPLConfig) error
	help     map[string]string
}

// REPL Config struct.
type REPLConfig struct {
	writer   io.Writer
	clientId uuid.UUID
}

// Get writer.
func (replConfig *REPLConfig) GetWriter() io.Writer {
	return replConfig.writer
}

// Get address.
func (replConfig *REPLConfig) GetAddr() uuid.UUID {
	return replConfig.clientId
}

// Construct an empty REPL.
func NewRepl() *REPL {
	r := REPL{make(map[string]func(string, *REPLConfig) error), make(map[string]string)}
	return &r
}

// Combines a slice of REPLs.
func CombineRepls(repls []*REPL) (*REPL, error) {
	new_repl := NewRepl()
	if len(repls) == 0 {
		return new_repl, nil
	}
	for _, repl := range repls {
		for cmd := range repl.commands {
			_, exist := new_repl.commands[cmd]
			if exist == true {
				return nil, errors.New("overlapping triggers")
			}
			new_repl.commands[cmd] = repl.commands[cmd]
			new_repl.help[cmd] = repl.help[cmd]
		}
	}
	return new_repl, nil
}

// Get commands.
func (r *REPL) GetCommands() map[string]func(string, *REPLConfig) error {
	return r.commands
}

// Get help.
func (r *REPL) GetHelp() map[string]string {
	return r.help
}

// Add a command, along with its help string, to the set of commands.
func (r *REPL) AddCommand(trigger string, action func(string, *REPLConfig) error, help string) {
	if strings.HasPrefix(trigger, ".") {
		fmt.Errorf("Cannot add meta commands")
		return
	}
	r.commands[trigger] = action
	r.help[trigger] = help
}

// Return all REPL usage information as a string.
func (r *REPL) HelpString() string {
	s := ""
	for cmd, helpstr := range r.help {
		s += cmd + ": " + helpstr + "\n"
	}
	return s
}

// Run the REPL.
func (r *REPL) Run(c net.Conn, clientId uuid.UUID, prompt string) {
	// Get reader and writer; stdin and stdout if no conn.
	var reader io.Reader
	var writer io.Writer
	if c == nil {
		reader = os.Stdin
		writer = os.Stdout
	} else {
		reader = c
		writer = c
	}
	scanner := bufio.NewScanner((reader))
	replConfig := &REPLConfig{writer: writer, clientId: clientId}
	// Begin the repl loop!
	for scanner.Scan() {
		line := scanner.Text()
		if line == "EOF" {
			break
		}
		elements := strings.Split(line, " ")
		cmd := elements[0]
		if cmd == ".help" {
			for available_cmds := range r.commands {
				io.WriteString(writer, available_cmds)
			}
		} else {
			function := r.commands[cmd]
			function(line, replConfig)
		}
		io.WriteString(writer, prompt)
	}
}

// cleanInput preprocesses input to the db repl.
func cleanInput(text string) string {
	panic("function not yet implemented");
}

func (r *REPL) RunChan(c chan string, clientId uuid.UUID, prompt string) {
	// Get reader and writer; stdin and stdout if no conn.
	writer := os.Stdout
	replConfig := &REPLConfig{writer: writer, clientId: clientId}
	// Begin the repl loop!
	io.WriteString(writer, prompt)
	for payload := range c {
		// Emit the payload for debugging purposes.
		io.WriteString(writer, payload+"\n")
		// Parse the payload.
		fields := strings.Fields(payload)
		if len(fields) == 0 {
			io.WriteString(writer, prompt)
			continue
		}
		trigger := cleanInput(fields[0])
		// Check for a meta-command.
		if trigger == ".help" {
			io.WriteString(writer, r.HelpString())
			io.WriteString(writer, prompt)
			continue
		}
		// Else, check user commands.
		if command, exists := r.commands[trigger]; exists {
			// Call a hardcoded function.
			err := command(payload, replConfig)
			if err != nil {
				io.WriteString(writer, fmt.Sprintf("%v\n", err))
			}
		} else {
			io.WriteString(writer, "command not found\n")
		}
		io.WriteString(writer, prompt)
	}
	// Print an additional line if we encountered an EOF character.
	io.WriteString(writer, "\n")
}
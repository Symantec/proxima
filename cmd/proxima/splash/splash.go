package splash

import (
	"bufio"
	"fmt"
	"github.com/Symantec/Dominator/lib/html"
	"io"
	"net/http"
)

type HtmlWriter interface {
	WriteHtml(writer io.Writer)
}

type Handler struct {
	Log HtmlWriter
}

func (h *Handler) ServeHTTP(
	w http.ResponseWriter, r *http.Request) {
	writer := bufio.NewWriter(w)
	defer writer.Flush()
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintln(writer, "<html>")
	fmt.Fprintln(writer, "<title>Proxima status page</title>")
	fmt.Fprintln(writer, "<body>")
	fmt.Fprintln(writer, "<center>")
	fmt.Fprintln(writer, "<h1><b>Proxima</b> status page</h1>")
	fmt.Fprintln(writer, "</center>")
	html.WriteHeaderNoGC(writer)
	fmt.Fprintln(writer, "<br>")
	h.Log.WriteHtml(writer)
	fmt.Fprintln(writer, "</body>")
	fmt.Fprintln(writer, "</html>")
}

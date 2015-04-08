package main

import (
    "bufio"
    "fmt"
    "log"
    "io"
    //"strconv"
    "strings"
    "unicode/utf8"
    "os/exec"
    "github.com/influxdb/influxdb/client"
)

/*
 * Take stream of 'zpool iostat -v' output and turn it into timeseries data for influxdb.
 * poolname.[mirror_N | raidz[123]_N].vdevname.alloc
 * poolname.[mirror_N | raidz[123]_N].vdevname.free
 * poolname.[mirror_N | raidz[123]_N].vdevname.nreads
 * poolname.[mirror_N | raidz[123]_N].vdevname.nwrites
 * poolname.[mirror_N | raidz[123]_N].vdevname.readbw
 * poolname.[mirror_N | raidz[123]_N].vdevname.writebw
 */

// isSpace reports whether the character is a Unicode white space character.
// We avoid dependency on the unicode package, but check validity of the implementation
// in the tests.
func isSpace(r rune) bool {
    if r <= '\u00FF' {
        // Obvious ASCII ones: \t through \r plus space. Plus two Latin-1 oddballs.
        switch r {
        case ' ', '\t', '\n', '\v', '\f', '\r':
            return true
        case '\u0085', '\u00A0':
            return true
        }
        return false
    }
    // High-valued ones.
    if '\u2000' <= r && r <= '\u200a' {
        return true
    }
    switch r {
    case '\u1680', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000':
        return true
    }
    return false
}

/* calcint adapted from http://www.maier-komor.de/mbuffer.html argument passing*/
func calcint(s string) int64 {
    var ch int
    var d float64

    n,err := fmt.Sscanf(s, "%f%c", &d, &ch)
    if n == 0 && err != nil {
        fmt.Printf("calcint: <%s> %d err=", s, n)
        fmt.Println(err)
    }
    switch n {
    case 2:
        switch (ch) {
        case 'k':
        case 'K':
            d *= 1024.0;
            return int64(d);
        case 'm':
        case 'M':
            d *= 1024.0*1024.0;
            return int64(d);
        case 'g':
        case 'G':
            d *= 1024.0*1024.0*1024.0;
            return int64(d);
        case 't':
        case 'T':
            d *= 1024.0*1024.0*1024.0*1024.0;
            return int64(d);
        default:
            return int64(d);
        }
    case 1:
        return int64(d);
    case 0:
        break;
    }
    return 0;
}


// ScanSpaceWords is a split function for a Scanner that returns each
// space-separated word of text, with surrounding spaces grouped. It will
// never return an empty string. The definition of space is set by
// unicode.IsSpace.
func ScanSpaceWords(data []byte, atEOF bool) (advance int, token []byte, err error) {
    const (
        stinit = iota 
        stspace = iota
        stword = iota)

    state := stinit
    // Scan until space, marking end of word or beginning of new word
    for width, i := 0,0; i < len(data); i += width {
        var r rune
        r, width = utf8.DecodeRune(data[i:])
        switch state {
        case stinit:
            if isSpace(r) {
                state = stspace
            } else {
                state = stword
            }
        case stword:
            if isSpace(r) {
                return i, data[0:i], nil
            }
        case stspace:
            if !isSpace(r) {
                return i, data[0:i], nil
            }
        }
    }
    // If we're at EOF, we have a final, non-empty, non-terminated word or space. Return it.
    if atEOF && len(data) > 0 {
        return len(data), data[0:], nil
    }
    // Request more data.
    return 0, nil, nil
}

func ReadInts(r io.Reader) {
    const (
        stinit = iota
        stbody = iota
    )
    sr_lines := bufio.NewScanner(r)

    type Res struct {
        depth int
        name[4] string
        namecnt[4] int
        alloc, free, nread, nwrite, readbw, writebw int64
    }

    var res Res
    var linearr[16] string

    series := []*client.Series{}

    c, err := client.NewClient(&client.ClientConfig{
    Host: "172.24.8.59:8086",
            Username: "admin",
            Password: "admin",
            Database: "zpool",
    })
    if err != nil {
            panic(err)
    }

    state := stinit
    res.depth = -1
    depth := 0
    skipone := 0
    for sr_lines.Scan() {
            line := sr_lines.Text()
            if len(line) == 0 {
                continue
            }
            switch state {
            case stinit:
                if line[0] == '-' {
                    res.depth = -1
                    state = stbody
                    depth = 0
                }
            case stbody:
                if line[0] == '-' {
                    res.depth = -1
                    state = stbody
                    depth = 0
                    if err := c.WriteSeries(series); err != nil {
                        panic(err)
                    }
    	            series = []*client.Series{}
                    break
                }
                if skipone == 1 {
                    skipone = 0
                    break
                }
                sr_words := bufio.NewScanner(strings.NewReader(line))
                sr_words.Split(ScanSpaceWords)
                wordpos := 0
                for sr_words.Scan() {
                    wordpos++;
                    if wordpos == 1 {
                        depth = 0
                        if line[0] == ' ' {
                            depth = len(sr_words.Text())
                        } else {
                            wordpos++
                        }
                    }
                    linearr[wordpos] = sr_words.Text()
                }
                if(wordpos < 14) {
                    skipone = 1
                    break
                }
                if(depth > res.depth) {
                    res.name[depth/2] = linearr[2]
                    res.namecnt[depth/2] = 0
                } else {
                    res.namecnt[depth/2]++
                }
                res.depth = depth
                leaf := 0
                if(linearr[4][0] != '-') {
                    res.alloc = calcint(linearr[4])
                    res.free = calcint(linearr[6])
                } else {
                    leaf = 1
                }
                res.nread = calcint(linearr[8])
                res.nwrite = calcint(linearr[10])
                res.readbw = calcint(linearr[12])
                res.writebw = calcint(linearr[14])
                name := res.name[0]
                for d := 1; d <= res.depth/2; d++ {
                    if leaf ==1 && d == res.depth/2  {
                        name += fmt.Sprintf(".%s", res.name[d])
                    } else {
                        name += fmt.Sprintf(".%s_%d", res.name[d], res.namecnt[d])
                    }
                }
                serie := &client.Series{
                    Name:    name + ".alloc",
                    Columns: []string{"value"},
                    Points: [][]interface{}{
                        {res.alloc},
                    },
                }
                series = append(series, serie) 
                serie = &client.Series{
                    Name:    name + ".free",
                    Columns: []string{"value"},
                    Points: [][]interface{}{
                        {res.free},
                    },
                }
                series = append(series, serie) 
                serie = &client.Series{
                    Name:    name + ".nread",
                    Columns: []string{"value"},
                    Points: [][]interface{}{
                        {res.nread},
                    },
                }
                series = append(series, serie) 
                serie = &client.Series{
                    Name:    name + ".nwrite",
                    Columns: []string{"value"},
                    Points: [][]interface{}{
                        {res.nwrite},
                    },
                }
                series = append(series, serie) 
                serie = &client.Series{
                    Name:    name + ".readbw",
                    Columns: []string{"value"},
                    Points: [][]interface{}{
                        {res.readbw},
                    },
                }
                series = append(series, serie) 
                serie = &client.Series{
                    Name:    name + ".writebw",
                    Columns: []string{"value"},
                    Points: [][]interface{}{
                        {res.writebw},
                    },
                }
                series = append(series, serie) 
            }
    }
}


func main() {

    tf := 
    "                                               capacity     operations    bandwidth\n" +
    "pool                                        alloc   free   read  write   read  write\n" +
    "------------------------------------------  -----  -----  -----  -----  -----  -----\n" +
    "tralala                                     3.70T  7.17T     50    369   855K  5.71M\n" +
    "  mirror                                    1.23T   594G     16     59   242K   971K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      6     12   129K   972K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      6     12   130K   972K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      6     12   130K   972K\n" +
    "------------------------------------------  -----  -----  -----  -----  -----  -----\n" +
    "lalala                                      3.70T  7.17T     50    369   855K  5.71M\n" +
    "  mirror                                    1.23T   594G     16     59   242K   971K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      6     12   129K   972K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      6     12   130K   972K\n" +
    "  mirror                                    1.23T   594G     16     71   241K  1009K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      6     13   129K  1009K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      6     13   130K  1009K\n" +
    "  mirror                                    1.23T   595G     16     59   241K   983K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      6     12   129K   983K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      6     12   130K   983K\n" +
    "  mirror                                    2.85G  1.81T      0     79  63.8K  1.33M\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      0     16  31.3K  1.33M\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      0     16  32.6K  1.33M\n" +
    "  mirror                                    1.15G  1.81T      0     52  26.5K   689K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      0     10  13.3K   678K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      0     10  13.2K   690K\n" +
    "  mirror                                    1.99G  1.81T      0     55  46.0K   946K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      0     12  23.7K   925K\n" +
    "    scsi-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx      -      -      0     12  22.4K   948K\n" +
    "------------------------------------------  -----  -----  -----  -----  -----  -----\n"

    ReadInts(strings.NewReader(tf))
    cmd := exec.Command("zpool", "iostat", "-v", "10")
    stdout, err := cmd.StdoutPipe()
    if err != nil {
        log.Fatal(err)
    }
    if err := cmd.Start(); err != nil {
        log.Fatal(err)
    }
    ReadInts(stdout)
}

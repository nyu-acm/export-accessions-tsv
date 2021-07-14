package main

import (
	"bufio"
	"fmt"
	"github.com/nyudlts/go-aspace"
	"os"
)

type IDs struct {
	Repos int
	Acc int
}

type Acc struct {
	Ids string
	Title string
	Repo string
	Resource string
}

var client *aspace.ASClient
var err error

func main() {
	client, err = aspace.NewClient("/etc/go-aspace.yml", "fade", 20)
	if err != nil {
		panic(err)
	}


	repoAccessions := []IDs{}

	for _, repositoryId := range []int{2,3,6} {
		accessionIds, err := client.GetAccessionIDs(repositoryId)
		if err != nil {
			panic(err)
		}

		for _, accessionId := range accessionIds {
			repoAccessions = append(repoAccessions, IDs{repositoryId, accessionId})
		}
	}

	chunks := chunkAccessions(repoAccessions, 8)
	fmt.Println(len(chunks))

	accChannel := make(chan []Acc)
	for _, chunk := range chunks {
		go getAccession(chunk, accChannel)
	}

	accs := []Acc{}
	for range chunks {
		chunk := <-accChannel
		accs = append(accs, chunk...)
	}

	f, _ := os.Create("accessions.tsv")
	defer f.Close()
	writer := bufio.NewWriter(f)

	for _, a := range accs {
		fmt.Print(".")
		writer.WriteString(fmt.Sprintf("%s\t%s\t%s\t%s\n", a.Ids, a.Title, a.Repo, a.Resource))
		writer.Flush()
	}
}

func getAccession(ids []IDs, accChan chan []Acc) {
	accs := []Acc{}
	for _,id := range ids {
		fmt.Print("*")
		acc, _ := client.GetAccession(id.Repos, id.Acc)
		a := Acc{getIds(acc), acc.Title, acc.Repository.Ref, getResource(acc.RelatedResources)}
		accs = append(accs, a)
	}
	accChan <- accs
}

func getResource(r []map[string]string) string{
	if len(r) > 0 {
		return r[0]["ref"]
	}
	return ""
}

func getIds(acc aspace.Accession) string {
	a := acc.ID0
	if acc.ID1 != "" {
		a = a + "." + acc.ID1
	}
	if acc.ID2 != "" {
		a = a + "." + acc.ID2
	}
	if acc.ID3 != "" {
		a = a + "." +acc.ID3
	}
	return a
}

func chunkAccessions(ids []IDs, n int) [][]IDs {
	var divided [][]IDs

	chunkSize := (len(ids) + n - 1) / n

	for i := 0; i < len(ids); i += chunkSize {
		end := i + chunkSize

		if end > len(ids) {
			end = len(ids)
		}

		divided = append(divided, ids[i:end])
	}
	return divided
}
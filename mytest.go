package main

import (
	"fmt"
	"time"

	ulid "github.com/oklog/ulid/v2"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/storage/key"
)

func main2() {
	path := "/Volumes/PascalsSSD/ArgosMiner/diskStorage"
	store := storage.NewDiskStorage(path)
	// k := "eKomiBewertungslinkVersand::Email wurde erfolgreich versendet-->eKomiBewertungslinkVersand::Daten in NPS ablegen"
	k := "eKomiBewertungslinkVersand::Agenturdaten beschaffen-->eKomiBewertungslinkVersand::Agenturdaten beschaffen"
	myKey := prefixString([]byte{0x01, 0x03}, k)
	encodedKey, _ := key.New(myKey, time.Now())
	store.Iterate(encodedKey[:8], func(key []byte, value func() ([]byte, error)) (bool, error) {
		var id ulid.ULID
		err := id.UnmarshalBinary(key[8:])
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println(time.UnixMilli(int64(id.Time())).Format(time.RFC3339))
		val, err := value()
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println("Value is ", storage.BytesToUint64(val))
		return true, nil
	})
}

func prefixString(prefix []byte, str string) []byte {
	return append(prefix, []byte(str)...)
}

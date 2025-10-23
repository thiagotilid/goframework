package goframework

import (
	"crypto/md5"
	"fmt"
	"regexp"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type Route struct {
	Id     uuid.UUID `bson:"_id"`
	Method string
	Path   string
	Module string
	Active bool
}

func NewRoute(r gin.RouteInfo, module string) *Route {
	re := regexp.MustCompile(`:([^\/]*)`)
	path := re.ReplaceAllString(r.Path, ".+")

	hash := md5.Sum([]byte(fmt.Sprintf("%s-%s-%s", module, r.Method, path)))
	id := uuid.NewMD5(uuid.NameSpaceOID, hash[:])

	return &Route{
		Id:     id,
		Method: r.Method,
		Path:   path,
		Module: module,
		Active: true,
	}
}

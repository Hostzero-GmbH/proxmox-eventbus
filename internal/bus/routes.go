package bus

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// runRouteManager periodically reconciles the embedded NATS server's cluster
// routes against the live /etc/pve/.members file.
//
// We poll at a low cadence instead of inotify-watching pmxcfs because .members
// is rewritten atomically and the cost of a fresh read is nanoseconds (tmpfs);
// polling keeps the failure mode trivial.
func (s *Server) runRouteManager(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	var last string
	var mu sync.Mutex
	for {
		mu.Lock()
		s.applyRoutes(&last)
		mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Server) applyRoutes(last *string) {
	log := s.opts.Logger
	peers, err := s.opts.Reader.Peers()
	if err != nil {
		if log != nil {
			log.Warn("route manager: read peers failed", "err", err)
		}
		return
	}
	ips := make([]string, 0, len(peers))
	for _, p := range peers {
		if p.IP == "" {
			continue
		}
		ips = append(ips, p.IP)
	}
	sort.Strings(ips)
	key := strings.Join(ips, ",")
	if key == *last {
		return
	}

	urls := make([]*url.URL, 0, len(ips)+len(s.opts.StaticRoutes))
	for _, ip := range ips {
		u, err := url.Parse(fmt.Sprintf("nats-route://%s:%d", ip, s.opts.ClusterPort))
		if err != nil {
			continue
		}
		urls = append(urls, u)
	}
	urls = append(urls, routesToURLs(s.opts.StaticRoutes)...)

	newOpts := *s.natsOpts
	newOpts.Routes = urls
	if err := s.ns.ReloadOptions(&newOpts); err != nil {
		if log != nil {
			log.Warn("route manager: ReloadOptions failed", "err", err)
		}
		return
	}
	s.natsOpts = &newOpts
	*last = key
	if log != nil {
		log.Info("route manager: routes updated", "peers", ips)
	}
}

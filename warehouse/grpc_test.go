package warehouse

import (
	"context"
	"github.com/hashicorp/yamux"
	"github.com/ory/dockertest/v3"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"strconv"
	"testing"
)

//var _ = Describe("Db", func() {
//	DescribeTable("ClauseQueryArgs", func(filterClauses []warehouse.FilterClause, expectedQuery string, expectedArgs []interface{}) {
//		query, args := warehouse.ClauseQueryArgs(filterClauses...)
//		Expect(query).To(Equal(expectedQuery))
//		Expect(args).To(Equal(expectedArgs))
//	},
//		Entry(nil, []warehouse.FilterClause{}, "", nil),
//		Entry(nil, []warehouse.FilterClause{{Clause: "id = <noop>", ClauseArg: 1}}, "id = $1", []interface{}{1}),
//		Entry(nil, []warehouse.FilterClause{{Clause: "id = <noop>", ClauseArg: 1}, {Clause: "val = <noop>", ClauseArg: 2}}, "id = $1 AND val = $2", []interface{}{1, 2}),
//		Entry(nil, []warehouse.FilterClause{{Clause: "id = ANY(<noop>)", ClauseArg: pq.Array([]interface{}{1})}, {Clause: "val = <noop>", ClauseArg: 2}}, "id = ANY($1) AND val = $2", []interface{}{pq.GenericArray{A: []interface{}{1}}, 2}),
//	)
//})

func TestGRPC(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

	err = (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "wh_schema_migrations",
	}).Migrate("warehouse")
	require.NoError(t, err)

	ctx := context.Background()

	tcpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	tcpAddress := net.JoinHostPort("", strconv.Itoa(tcpPort))

	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		listener, err := net.Listen("tcp", tcpAddress)
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, listener.Close())
		})
		conn, err := listener.Accept()
		require.NoError(t, err)

		session, err := yamux.Client(conn, yamux.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, session.Close())
		})

		grpcConn, err := grpc.Dial("",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context context.Context, target string) (net.Conn, error) {
				return session.Open()
			}),
		)
		t.Cleanup(func() {
			require.NoError(t, grpcConn.Close())
		})
		return nil
	})
}

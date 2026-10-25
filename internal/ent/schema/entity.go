package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

// Entity 是“实际存储的那一份内容”。
//
// File 和 Entity 分开，是为了以后能做内容去重：
// 同一个 Entity 可以被多个 File 引用，磁盘上只存一份。
type Entity struct {
	ent.Schema
}

// Fields 是 Entity 自己的列。
func (Entity) Fields() []ent.Field {
	return []ent.Field{
		// 磁盘上的文件名（当前是本机存储的 key）。
		// 用 Unique 保证同一份内容不会被登记成两条 Entity。
		field.String("storage_key").Unique().NotEmpty(),

		// 内容大小，单位字节。下载时用来设置 Content-Length。
		field.Int64("size").NonNegative(),

		// 存储策略。P2 只有 local，P3 接 S3 时可以写 "s3"。
		field.String("policy").Default("local"),

		// 有多少个 File 引用这份内容。删除 File 时减一，减到 0 才删磁盘文件。
		field.Int("ref_count").Default(1).NonNegative(),
	}
}

// Edges 是 Entity 和其它表的关系。
func (Entity) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("files", File.Type).Ref("entity"),
	}
}

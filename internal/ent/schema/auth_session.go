package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

// AuthSession 是一次登录产生的会话。
//
// 客户端拿到原始 Token；数据库里只保存它的 SHA-256，
// 这样即使数据库泄露，也不能直接拿表里的值冒充用户登录。
type AuthSession struct {
	ent.Schema
}

// Fields 是 AuthSession 自己的列。
func (AuthSession) Fields() []ent.Field {
	return []ent.Field{
		// 原始 Token 的 SHA-256（十六进制）。Unique 让一次请求最多命中一个会话。
		field.String("token_hash").Unique().NotEmpty(),

		// 到这个时间点之后，Token 不再有效。
		field.Time("expires_at"),

		// 会话所属用户。它和下面的 user 边共用同一列，创建后不允许修改。
		field.Int("user_id").Immutable(),
	}
}

// Edges 是 AuthSession 和其它表的关系。
func (AuthSession) Edges() []ent.Edge {
	return []ent.Edge{
		// 每个会话必须属于一个用户，生成的 user_id 列就是文件归属检查用到的用户 ID。
		edge.From("user", User.Type).Ref("sessions").Unique().Field("user_id").Immutable().Required(),
	}
}

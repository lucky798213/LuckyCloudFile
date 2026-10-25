package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

// User 是登录用户。本阶段不做注册，账号由开发初始化命令显式创建。
type User struct {
	ent.Schema
}

// Fields 是 User 自己的列。
func (User) Fields() []ent.Field {
	return []ent.Field{
		// 用户名全局唯一。登录时按它查找用户。
		field.String("username").Unique().NotEmpty(),

		// bcrypt 生成的密码哈希。字符串里包含盐和代价参数，
		// 数据库里绝不保存用户输入的明文密码。
		field.String("password_hash").NotEmpty(),
	}
}

// Edges 是 User 和其它表的关系。
func (User) Edges() []ent.Edge {
	return []ent.Edge{
		// 一个用户可以有多个登录会话；退出时只删当前那一个。
		edge.To("sessions", AuthSession.Type),
		// 一个用户拥有多个文件。
		edge.To("files", File.Type),
	}
}

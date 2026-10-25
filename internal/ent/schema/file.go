package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

// File 是用户看到的逻辑文件。
//
// 它只记录“叫什么、放在哪、属于谁、内容是哪一份”，
// 真正的字节内容交给 Entity。这样一个内容可以被多个文件名引用（以后做秒传/复制）。
type File struct {
	ent.Schema
}

// Fields 是 File 自己的列。
func (File) Fields() []ent.Field {
	return []ent.Field{
		// 服务端生成的公开 ID。用字符串主键，上传时返回给客户端的就是它，
		// 重启后 URL 里的 :id 仍然能查到同一条记录。
		field.String("id").Unique().Immutable(),

		// 用户看到的原始文件名，只用于展示和下载时的 Content-Disposition。
		field.String("name").NotEmpty(),

		// 父目录。本阶段还没有目录表，先用逻辑路径表示（例如 "/"、"/photos"）。
		field.String("parent").Default("/"),

		// 文件所有者的用户 ID。它和下面的 owner 边共用同一列；
		// 上传后不允许修改，防止文件被转移到别的用户名下。
		field.Int("owner_id").Immutable(),
	}
}

// Edges 是 File 和其它表的关系。
func (File) Edges() []ent.Edge {
	return []ent.Edge{
		// 一个 File 必须对应一个 Entity；Entity 可以被多个 File 引用。
		edge.To("entity", Entity.Type).Unique().Required(),

		// 文件所有者。生成的 owner_id 列只能由服务端根据登录会话填写，
		// 下载和删除时也必须用当前用户的 ID 一起查询。
		edge.From("owner", User.Type).Ref("files").Unique().Field("owner_id").Immutable().Required(),
	}
}

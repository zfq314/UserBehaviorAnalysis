package com.atguigu.hotitems_analysis.pojo
//用户行为样例类增加注释
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int,
                        behavior: String, timestamp: Long) {

}

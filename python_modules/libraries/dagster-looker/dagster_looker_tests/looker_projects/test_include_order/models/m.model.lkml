connection: "warehouse"

include: "../views/*.view"
include: "/refinements/b.layer.lkml"
include: "/refinements/a.layer.lkml"
# Matches the two files above again, so a.layer.lkml stays the last refinement of
# `orders`, and does not match files of imported projects.
include: "/**/*.layer"
include: "//shared/views/users.view"

view: +customers {
  sql_table_name: analytics.customers ;;
}

explore: orders {}

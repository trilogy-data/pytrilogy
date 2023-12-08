<template lang="html">
    <Tabs>
        <Tab title="Results">
            <TableComponent :data="data" :fields="fields" />
        </Tab>
        <tab title="SQL"><SQL :query="gen_sql"/></tab>
    </Tabs>
</template>
  
<script>
import Tab from './Tab.vue'
import Tabs from './Tabs.vue'
import TableComponent from './TableComponent.vue'
import SQL from './SQL.vue'
export default {
    props: {
        // 
        data: {
            type: Array,
        },
        fields: {
            type: Array,
        },
        gen_sql: {
            type: String,
        },
    },
    components: {
        Tab,
        Tabs,
        TableComponent,
        SQL
    },
}
</script>
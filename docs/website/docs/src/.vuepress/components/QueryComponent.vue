<template>
    <h3>{{ title }}</h3>
    <SQL  :query=query></SQL>
    <LoadingButton :loading="loading" class="submit-button" v-if="data.length == 0" @click="getData" type='submit'>Run</LoadingButton>
    <button v-else @click="show_results = !show_results">Toggle results</button>
    <div v-if="error">{{ error }}</div>
    <ResultComponent v-if="show_results" :id="title"  :data="data" :fields="headers" :gen_sql="gen_sql" />
</template>
    
<style>

</style>
<script>
import axios from 'axios';
// import TableComponent from './TableComponent.vue';
import ResultComponent from './ResultComponent.vue';
import LoadingButton from './LoadingButton.vue';
import SQL from './SQL.vue'

export default {
    data() {
        return {
            data: [],
            headers: [],
            show_results: false,
            error: null,
            gen_sql: '',
            loading: false,

        };
    },
    components: {
        ResultComponent,
        SQL,
        LoadingButton,
        // TableComponent,
    },
    props: {
        query: String,
        title: String,
        dependencies: {
            type: Array,
            default: () => [],
        }

    },

    methods: {
        async getData() {
            this.error = null;
            this.loading = true;
            let queries = this.dependencies.concat([this.query]);
            const joinedString = queries.join(' ');
            try {
                const response = await axios.post(
                    "https://preql-demo-backend-cz7vx4oxuq-uc.a.run.app/query",
                    { 'model': 'titanic', 'query': joinedString }
                );
                this.headers = response.data.headers;
                this.data = response.data.results;
                this.gen_sql = response.data.generated_sql;
                this.show_results = true;
                this.loading = false;
            } catch (error) {
                this.error = error;
                this.loading = false;
                console.log(error);
            }
        },
    },
};
</script>
<style></style>

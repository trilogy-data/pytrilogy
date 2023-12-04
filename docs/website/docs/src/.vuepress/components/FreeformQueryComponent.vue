<template>
    <h3>{{ title }}</h3>
    <div class="language-sql">
        <textarea ref="sql" v-model="user_query"
        rows = "5"
        placeholder="select 1 -> one;"
            @keydown.enter.ctrl.exact.prevent="getData"/>
    </div>

    <button class="submit-button" @click="getData" type='submit'>Run (ctrl+enter)</button>
    <!-- <button v-else @click="show_results = !show_results">Toggle results</button> -->
    <div v-if="error">{{ error }}</div>
    <ResultComponent v-else :id="title" :data="data" :fields="headers" :gen_sql="gen_sql" />
</template>
    
<style>
/* CSS */
.submit-button {
    background-color: #c2fbd7;
    border-radius: 100px;
    box-shadow: rgba(44, 187, 99, .2) 0 -25px 18px -14px inset, rgba(44, 187, 99, .15) 0 1px 2px, rgba(44, 187, 99, .15) 0 2px 4px, rgba(44, 187, 99, .15) 0 4px 8px, rgba(44, 187, 99, .15) 0 8px 16px, rgba(44, 187, 99, .15) 0 16px 32px;
    color: green;
    cursor: pointer;
    display: inline-block;
    font-family: CerebriSans-Regular, -apple-system, system-ui, Roboto, sans-serif;
    padding: 5px 20px;
    text-align: center;
    text-decoration: none;
    transition: all 250ms;
    border: 0;
    font-size: 12px;
    user-select: none;
    -webkit-user-select: none;
    touch-action: manipulation;
}

.submit-button:hover {
    box-shadow: rgba(44, 187, 99, .35) 0 -25px 18px -14px inset, rgba(44, 187, 99, .25) 0 1px 2px, rgba(44, 187, 99, .25) 0 2px 4px, rgba(44, 187, 99, .25) 0 4px 8px, rgba(44, 187, 99, .25) 0 8px 16px, rgba(44, 187, 99, .25) 0 16px 32px;
    transform: scale(1.05) rotate(-1deg);
}
</style>
<script>
import axios from 'axios';
import ResultComponent from './ResultComponent.vue';
import hljs from 'highlight.js/lib/core';
import sql from 'highlight.js/lib/languages/sql';
import 'highlight.js/styles/github.css';
hljs.registerLanguage('sql', sql);

export default {
    data() {
        return {
            data: [],
            headers: [],
            show_results: true,
            error: null,
            gen_sql: '',
            user_query: 'select 1 -> one;',

        };
    },
    components: {
        ResultComponent,
    },
    props: {
        query: String,
        title: String,
        dependencies: {
            type: Array,
            default: () => [],
        }

    },
    mounted() {
        // this.highlightSQL()
    },

    methods: {
        // keyHandler(e) {
        //     if (e.key === 'Enter') {
        //         e.preventDefault()
                
        //     }
            

        // },
        highlightSQL() {
            if (this.$refs.sql) {
                hljs.highlightElement(this.$refs.sql)
            }
        },
        async getData() {
            this.error = null;
            // this.highlightSQL();
            let queries = this.dependencies.concat([this.user_query]);
            const joinedString = queries.join(' ');
            try {
                const response = await axios.post(
                    "https://preql-demo-backend-cz7vx4oxuq-uc.a.run.app/query",
                    { 'model': 'titanic', 'query': joinedString }
                );
                this.headers = response.data.headers;
                this.data = response.data.results;
                this.gen_sql = response.data.generated_sql;
            } catch (error) {
                if (error.response && error.response.status== 422) {
                    this.error = error.response.data;
                }
                else {
                    this.error = error;
                }
            }
        },
    },
};
</script>
<style>
textarea {
    width: 100%;
    min-width: 400px;
    /* border-bottom: 10px; */
    margin-bottom: 10px;
    padding-bottom: 55px;
}
</style>

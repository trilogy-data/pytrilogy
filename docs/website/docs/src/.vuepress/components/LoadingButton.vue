<template>
    <button :class="{
        'vue-loading-button': true,
        'submit-button': true,
        'loading': loading,
    }" :disabled="loading" type="button">
        <slot>{{ text }}</slot>
        <span class="spinner">
            <span></span>
            <span></span>
            <span></span>
            <span></span>
        </span>
    </button>
</template>
  
<script>
export default {
    name: "VueLoadingButton",
    props: {
        text: {
            type: String,
        },
        loading: {
            type: Boolean,
            required: false,
            default: false
        },
        styled: {
            type: Boolean,
            required: false,
            default: false
        }
    }
};
</script>
  
<style scoped>
/* reset */
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

button::-moz-focus-inner {
    border-style: none;
    padding: 0;
}

button:-moz-focusring {
    outline: 1px dotted ButtonText;
}

/* loading styles */
button {
    position: relative;
    -webkit-transition: all 0.2s;
    transition: all 0.2s;
    transition-timing-function: ease-in;
}

.spinner {
    line-height: 1.00;
    position: absolute;
    top: 50%;
    left: auto;
    right: 1.7rem;
    margin: -0.5em;
    opacity: 0;
    transition-property: padding, opacity;
    transition-duration: 0.2s, 0.2s;
    transition-timing-function: ease-in, ease;
    transition-delay: 0s, 0.2s;
}

.spinner span {
    box-sizing: border-box;
    display: inline-block;
    position: absolute;
    right: 0;
    top: 0.15rem;
    width: 1rem;
    height: 1rem;
    opacity: 1;
    border: 3.4px solid #888;
    border-radius: 50%;
    animation: spinner 1.2s cubic-bezier(0.5, 0, 0.5, 1) infinite;
    border-color: #888 transparent transparent transparent;
}

.spinner span:nth-child(1) {
    animation-delay: 0.45s;
}

.spinner span:nth-child(2) {
    animation-delay: 0.3s;
}

.spinner span:nth-child(3) {
    animation-delay: 0.15s;
}

.loading {
    padding-right: 3rem !important;
}

.loading .spinner {
    opacity: 1;
}

.loading .spinner span {
    opacity: 1;
}

button:not(:disabled) {
    transition-delay: 0.2s;
}

button:not(:disabled) .spinner span {
    box-shadow: 0 0 0 0.2rem #888 inset;
    border: 7.4px solid transparent;
    -webkit-transition: all 0.4s;
    transition: all 0.4s;
}

button:not(:disabled) .spinner span:nth-child(1) {
    transform: rotate(0deg) !important;
}

button:not(:disabled) .spinner span:nth-child(2) {
    transform: rotate(90deg) !important;
}

button:not(:disabled) .spinner span:nth-child(3) {
    transform: rotate(180deg) !important;
}

button:not(:disabled) .spinner span:nth-child(4) {
    transform: rotate(270deg) !important;
}

@keyframes spinner {
    0% {
        transform: rotate(0deg);
    }

    100% {
        transform: rotate(360deg);
    }
}
</style>
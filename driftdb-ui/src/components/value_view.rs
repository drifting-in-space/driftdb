use serde_json::Value;
use yew::{function_component, Properties, Html, html};


fn render_value(value: &Value) -> Html {
    match value {
        Value::Object(object) => html! {
            <>
                <span class="text-gray-400">{'{'}</span>
                <ul>
                {for object.iter().enumerate().map(|(i, (key, value))| {
                    html! {
                        <li class="pl-5">
                            <span class="text-gray-400">{'"'}</span>
                            <span class="text-blue-600">{key}</span>
                            <span class="text-gray-400">{'"'}</span>
                            <span class="text-gray-400">{": "}</span>
                            {render_value(value)}
                            {
                                if i < object.len() - 1 {
                                    html! {
                                        <span class="text-gray-400">{", "}</span>
                                    }
                                } else {
                                    html! {}
                                }
                            }
                        </li>
                    }
                })}
                </ul>
                <span class="text-gray-400">{'}'}</span>
            </>
        },

        Value::Number(number) => html! {
            <span class="text-yellow-600">{number}</span>
        },

        Value::Array(array) => html! {
            <>
                <span class="text-gray-400">{'['}</span>
                {for array.iter().enumerate().map(|(i, value)| {
                    html! {
                        <>
                            {render_value(value)}
                            {
                                if i < array.len() - 1 {
                                    html! {
                                        <span class="text-gray-400">{", "}</span>
                                    }
                                } else {
                                    html! {}
                                }
                            }
                        </>
                    }
                })}
                <span class="text-gray-400">{']'}</span>
            </>
        },

        Value::String(value) => html! {
            <>
                <span class="text-gray-400">{'"'}</span>
                <span class="text-green-600">{value}</span>
                <span class="text-gray-400">{'"'}</span>
            </>
        },
        
        Value::Bool(value) => html! {
            <span class="text-pink-600">{value}</span>
        },

        Value::Null => html! {
            <span class="text-red-400">{"null"}</span>
        },
    }
}

#[derive(Clone, Properties, PartialEq)]
pub struct PrettyJsonProps {
    pub value: Value,
}


#[function_component]
pub fn PrettyJson(props: &PrettyJsonProps) -> Html {
    log::info!("render_value: {:?}", props.value);
    let PrettyJsonProps { value } = props;

    html! {
        <div class="text-sm font-mono">
            {render_value(&value)}
        </div>
    }
}
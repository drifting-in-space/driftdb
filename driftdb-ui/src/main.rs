use crate::sync_store::use_sync_store;
use log::Level;
use serde::Deserialize;
use yew::prelude::*;
use yew_router::prelude::*;
use crate::components::value_view::PrettyJson;

pub mod sync_store;
mod components;
mod websocket;

#[derive(Clone, Routable, PartialEq)]
enum Route {
    #[at("/")]
    Home,
    #[at("/console")]
    Console,
}

#[derive(Deserialize)]
struct ConsoleQuery {
    db_url: String,
}

#[function_component]
fn Console() -> Html {
    let location = use_location().unwrap();
    let ConsoleQuery { db_url } = location.query().unwrap();

    // TODO: this is inefficient.
    let subjects = use_sync_store(&db_url).store().subjects().clone();

    html! {
        <div>
            {"console"}<br />
            {db_url}

            <ul>
                {for subjects.iter().map(|(key, value_log)| {
                    html! {
                        <li>
                            {key}
                            <ul>
                                {for value_log.values.iter().map(|value| {
                                    html! {
                                        <li>
                                            {value.seq.0} {"-"}
                                            <PrettyJson value={value.value.clone()} />
                                        </li>
                                    }
                                })}
                            </ul>
                        </li>
                    }
                })}
            </ul>

        </div>
    }
}

#[function_component]
fn Home() -> Html {
    html! {
        <div>
            {"Go to /console?db_url=..."}<br />
            <a href="/console?db_url=ws://localhost:8080">{"localhost"}</a>
        </div>
    }
}

fn switch(route: Route) -> Html {
    match route {
        Route::Home => html! { <Home /> },
        Route::Console => html! { <Console /> },
    }
}

#[function_component]
fn App() -> Html {
    html! {
        <div>
            <BrowserRouter>
                <Switch<Route> render={switch} /> // <- must be child of <BrowserRouter>
            </BrowserRouter>
        </div>
    }
}

fn main() {
    console_log::init_with_level(Level::Debug).expect("Error instantiating console_log.");

    yew::Renderer::<App>::new().render();
}

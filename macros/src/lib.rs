#![allow(dead_code)]

use proc_quote::quote;

#[proc_macro_derive(DbName, attributes(name, flags, table))]
pub fn derive_db_name(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let input = syn::parse_macro_input!(input as syn::DeriveInput);
	let name = &input.ident;

	let crate_name = crate_name();

	let mut db_name = None;
	let mut flags = None;
	let mut table = None;
	for attr in input.attrs {
		let args = attr.meta.require_list().unwrap();
		match &attr.path().get_ident().unwrap().to_string() as &str {
			"name" => {
				let lit = args.parse_args::<syn::LitStr>().unwrap();
				db_name = Some(syn::LitByteStr::new(format!("{}\0", lit.value()).as_bytes(), lit.span()));
			},
			"flags" => { flags = Some(args.parse_args::<syn::Expr>().unwrap()); },
			"table" => { table = Some(args.parse_args::<syn::Type>().unwrap()); },
			_ => unreachable!(),
		}
	}

	let flags = flags.map_or_else(|| quote!(), |x| quote!(fn flags() -> #crate_name::enumflags2::BitFlags<#crate_name::lmdb::DbFlags> { #x.into() }));
	let db_name = db_name.map_or_else(|| quote!(&::std::concat!(::std::module_path!(), "::", ::std::stringify!(#name), "\0").as_bytes()), |x| quote!(#x));//syn::LitByteStr::new(format!("{}\0", name).as_bytes(), name.span()));

	quote!(
		impl #crate_name::DbName for #name {
			type Table<'tx, 'env: 'tx, TX: #crate_name::Transaction<'env> + 'tx> = #table;
			const NAME: &'static [u8] = #db_name;
			#flags
		}
	).into()
}

fn crate_name() -> proc_macro2::TokenStream {
	let into_ident = |x| match x {
		proc_macro_crate::FoundCrate::Itself => quote! { crate },
		proc_macro_crate::FoundCrate::Name(x) => { let name = syn::Ident::new(&x, proc_macro2::Span::call_site()); quote! { ::#name } },
	};
	let indent = proc_macro_crate::crate_name("batadase").ok().map(into_ident);
	match indent {
		Some(indent) => quote! { #indent },
		None => quote! { crate },
	}
}

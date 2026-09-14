# BlueSky Circuit Breaker

A week or so ago, [BlueSky posted](https://bsky.app/profile/safety.bsky.app/post/3mukm26duos2c) about penalizing dogpiling and harassment on the platform. And while I think that's absolutely a great idea, I don't love the fact that it seems to be focused on after-the-fact enforcement. The BlueSky/Atmosphere philosophy seems like it should be focused on user control of their experience, managing their use of specific safety mechanisms.

So, in the spirit of "making things is the best way to complain", I built a very rough "BlueSky circuit breaker": [https://github.com/ajdecon/bsky-circuit-breaker](https://github.com/ajdecon/bsky-circuit-breaker)

**This is *not at all production-ready****.* I have a day job and it's not maintaining mod infrastructure. And I know from colleagues how much work that really is. This is sincerely just a proof-of-concept in the true sense: throw-away code to show a given idea may work.

This tiny tool demonstrates a *proof of concept* for:

-   Locking a post's interactions when too many people interact with it
-   Locking it when too many people interact at a given rate (interactions/hour)
-   Locking a post when a person with tons of followers quote-post it
-   Locking a post if you're getting "ratio'd" with too many replies vs likes

Experimentally, the tool also includes an option to call out to a local LLM to do sentiment analysis on replies. Things like "does this contain slurs", or "is it weirdly hostile to the original post author", or "is the reply completely off-topic". This is really r*eally* imperfect, but potentially useful to folks like me who are small-ish posters with day jobs who don't want to be caught in an accidental reply storm.

The script itself is a one-and-done affair: it runs, analyzes your feed, takes action if you allow it, and exits. On initial development I envisioned running this in a cron-like setup. I will probably run this, or something like it, on a once-an-hour schedule on a personal VM.

A production-like service should probably be more robust, but like I said: this is a proof-of-concept.

In any case, this is what I'd like to see social media moderation look like: tools to control interactions from others, including "oh God" circuit breakers for when you get way more attention than you expected.

IMHO, this kind of circuit breaker should be available be default! Most social media accounts are small, after all. While I recognize the challenges of responding instantly to dogpiles — distributed systems are hard — a cron-like function that scans accounts and performs post locks based on user config should not be too onerous.

Anyway, in the spirit of open source: here's the code! Do what you like.

Disclaimer: the design and much of the initial code of this script was me, directly. I then enlisted the help of qwen3-coder:30b for minor code fixes, in particular atproto API bits. What can I say, I was more interested in the logic than the external APIs.

Regardless, the code is MIT-licensed. I'm not sure it actually is good for much *directly*, but it might be inspiring for someone who wants to build circuit breakers into real apps. We can only hope.
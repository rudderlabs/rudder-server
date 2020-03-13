import { ButtonPrimary } from '@components/common/button';
import SourceIcon from '@components/icons/sourceIcon';
import { ISourceDefintion } from '@stores/sourceDefinitionsList';

// @ts-ignore
import raw from 'raw.macro';
import * as React from 'react';
import ReactMarkdown from 'react-markdown';
import { Link } from 'react-router-dom';

import theme from '@css/theme';
import { Flex } from '../../common/misc';
import { Header, TextDiv } from '../../common/typography';

// @ts-ignore
export const markdown = {
  Android: raw('./Android.md'),
  iOS: raw('./iOS.md'),
  Javascript: raw('./Javascript.md'),
  Unity: raw('./Unity.md'),
};

export interface ISourceConfigureProps {
  sourceDef?: ISourceDefintion;
}
export interface ISourceConfigureState {
  markdown: string;
}

export default class SourceConfigure extends React.Component<
  ISourceConfigureProps,
  ISourceConfigureState
> {
  constructor(props: ISourceConfigureProps) {
    super(props);
    this.state = {
      markdown: '',
    };
  }

  public render() {
    const { sourceDef } = this.props;
    return (
      <div className="p-l-lg">
        <Flex className="m-b-lg p-b-md b-b-grey">
          <SourceIcon
            source={sourceDef!.name}
            height={theme.iconSize.large}
            width={theme.iconSize.large}
          ></SourceIcon>
          <div className="m-l-md">
            <Header color={theme.color.black}>{sourceDef!.name}</Header>
          </div>
        </Flex>
        <div>
          <Link to={`/sources/setup?sourceDefId=${sourceDef!.id}`}>
            <ButtonPrimary className="m-b-lg">Configure</ButtonPrimary>
          </Link>
          <ReactMarkdown source={(markdown as any)[sourceDef!.name]} />
        </div>
      </div>
    );
  }
}

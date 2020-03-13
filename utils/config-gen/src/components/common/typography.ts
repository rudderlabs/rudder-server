import styled from 'styled-components';

interface IProps {
  fontSize?: string;
  color?: string;
}

const label = (props: any) => `
  font-family: Noto Sans;
  font-style: normal;
  font-weight: 500;
  font-size: 16px;
  line-height: 22px;
  color: ${props.color || props.theme.color.black};
`;
export const Label = styled.span`
  ${label}
`;

export const LabelDiv = styled.div`
  ${label}
`;

export const LabelSmall = styled.div`
  ${label}
  font-size: 12px;
  color: ${props => props.color};
`;

export const LabelMedium = styled.div`
  ${label}
  font-size: 14px;
`;

const text = (props: any) => `
  font-family: Noto Sans;
  font-style: normal;
  font-weight: normal;
  font-size: ${props.fontSize || '12px'};
  line-height: 20px;
  color: ${props.color || props.theme.color.black};
`;

export const Text = styled.span<IProps>`
  ${text}
`;

export const TextDiv = styled.div<IProps>`
  ${text}
`;

const header = (props: any) => `
  font-family: Noto Sans;
  font-style: normal;
  font-weight: normal;
  font-size: 24px;
  color: ${props.color || props.theme.color.primary};
`;

export const Header = styled.span`
  ${header}
`;

export const HeaderDiv = styled.div`
  ${header}
`;

const subHeader = (props: any) => `
font-family: Noto Sans;
font-style: normal;
font-weight: 600;
line-height: 22px;
font-size: 16px;
color: ${props.color || props.theme.color.primary};
`;

export const SubHeaderDiv = styled.div`
  ${subHeader}
`;
